#
# TODO:
# 3. Encrypt the contents of containers
#
import sys, os
import base64
from cStringIO import StringIO
import struct
import re
import traceback

from Nodes import Directory
import Nodes
import Container
from BlockCache import BlockCache
from Block import Block
from Database import *
from StreamAdapter import *
import manent.utils.Digest as Digest

class SpecialOStream(OStreamAdapter):
	"""
	This ostream writes its data to a stream of containers
	"""
	def __init__(self,backup,code):
		OStreamAdapter.__init__(self, backup.container_config.blockSize())
		self.backup = backup
		self.code = code
	def write_block(self,data):
		#print "adding block of code", self.code, "length", len(written)
		digest = Digest.dataDigest(data)
		self.backup.container_config.add_block(data,digest,self.code)

class SpecialIStream(IStreamAdapter):
	"""
	This istream reads its data from a stream of containers
	"""
	def __init__(self,backup,digests):
		IStreamAdapter.__init__(self)
		self.backup = backup
		self.digests = digests
	
	def read_block(self):
		if len(self.digests)==0:
			return ""
		(idx,digest) = self.digests[0]
		self.digests = self.digests[1:]
		data = self.backup.blocks_cache.load_block(digest,idx)
		return data

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self,global_config,label):
		self.global_config = global_config
		self.label = label

		self.db_config = DatabaseConfig(self.global_config,"manent.%s.db"%self.label)
		self.txn_handler = TransactionHandler(self.db_config)

		self.open_files_dbs = {}
	#
	# Three initialization methods:
	# Creation of new Backup, loading from live DB, loading from backups
	#
	def configure(self,data_path,container_type,container_params):
		#print "Creating backup", self.label, "type:", container_type, container_params
		self.data_path = data_path
		self.container_type = container_type
		self.container_params = container_params

	def remove(self):
		print "Removing backup", self.label
		try:
			self.db_config.remove_database()
		finally:
			self.db_config.close()

	def create(self):
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.txn_handler.commit()
		except:
			self.txn_handler.abort()
			traceback.print_exc()
			raise
		finally:
			self.blocks_db.close()
			self.db_config.close()

	def reconstruct(self,data_path,container_type,container_params):
		self.configure(data_path,container_type,container_params)
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.blocks_cache = BlockCache(self)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)

			self.do_reconstruct()
			self.txn_handler.commit()
		except:
			self.txn_handler.abort()
			traceback.print_exc()
			raise
		finally:
			self.blocks_db.close()
			self.blocks_cache.close()
			self.container_config.close()
			self.db_config.close()
			
	def do_reconstruct(self):
		print "Reconstructing backup", self.label, "type:", self.container_type, self.container_params
		self.inodes_db = {}
		
		self.root = Directory(self,None,self.data_path)

		#
		# Reconstruct the containers dbs
		#
		print "Reconstructing container config"
		self.container_config.reconstruct()
		#
		# Reconstruct the blocks db
		#
		print "Reconstructing blocks database:",
		for idx in range(0,self.container_config.num_containers()):
			print " ",idx,
			container = self.container_config.get_container(idx)
			for (digest,size,code) in container.blocks:
				if code == Container.CODE_DATA:
					block = Block(self,digest)
					block.add_container(idx)
					block.save()
					#print base64.b64encode(digest), "->", block.containers
			self.container_config.release_container(idx)
		print
	#
	# Functionality for scan mode
	#
	def scan(self):
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)

			self.do_scan()
			self.txn_handler.commit()
		except:
			self.txn_handler.abort()
			traceback.print_exc()
			raise
		finally:
			for key,files_db in self.open_files_dbs.iteritems():
				files_db.close()
			self.blocks_db.close()
			self.container_config.close()
			self.db_config.close()
		
	def do_scan(self):
		#
		# Check in which increments we should look
		#
		prev_increments = self.container_config.prev_increments()
		prev_files_dbs = []
		prev_nums = []
		last_based_increment = None
		for idx in prev_increments:
			f_increment = self.container_config.increments[idx]
			if f_increment.finalized:
				# At most one of the prev_increments is supposed to be
				# finalized, and the final one!
				self.__load_files_db(idx)
				if f_increment.base_index != None:
					last_based_increment = f_increment
			if self.__files_db_loaded(idx):
				prev_files_dbs.append(self.__load_files_db(idx))
				prev_nums.append((len(prev_nums),0,Nodes.NODE_DIR))
		prev_nums.reverse()
		print "Previous increments are: ", prev_increments, prev_nums

		base_index = None
		new_increment = False
		
		if last_based_increment != None:
			print "last based increment", last_based_increment
			# Only finalized increments can change the base!
			base_index = last_based_increment.base_index
			base_diff = last_based_increment.base_diff
			if base_index == None:
				# The previous increment is not based on anybody - OK, we base on it
				base_index = prev_increments[-1]
				(idx,node_num,code) = prev_nums[-1]
				# This will upgrade all the nodes in this increment to BASED status
				# for the updating
				prev_nums[-1] = (idx,node_num,Nodes.NODE_DIR_BASED)
			elif base_diff>0.5:
					# The previous increment is based on somebody, but too big - OK,
					# we'll make a new one
					print "Difference too big. Starting a new increment"
					new_increment = True
					#base_index = None
			else:
				print "Reusing the same increment", base_index
				prev_nums.append((len(prev_nums),0,Nodes.NODE_DIR_BASED))
				prev_files_dbs.append(self.__load_files_db(base_index))
			print "Basing this increment on", base_index
		
		base_files_db = None
		if base_index != None:
			base_files_db = self.__load_files_db(base_index)
		
		#
		# Do the real work of scanning
		#
		increment = self.container_config.start_increment(base_index)
		new_files_db = self.__open_files_db(increment)
		root = Directory(self,None,self.data_path)
		root.code = Nodes.NODE_DIR
		ctx = ScanContext(self,root,base_files_db,prev_files_dbs,new_files_db)
		ctx.new_increment = new_increment
		root.set_num(ctx.next_num())
		root.scan(ctx,prev_nums)

		base_diff = None
		if base_index != None:
			base_diff = 0.0
			if ctx.total_nodes != 0:
				base_diff = ctx.changed_nodes / float(ctx.total_nodes)
		print "Diff from previous increment:", base_diff, "which is", ctx.changed_nodes, "out of", ctx.total_nodes
		
		#
		# Save the files db
		#
		print "Exporting the files db"
		s = SpecialOStream(self,Container.CODE_FILES)
		for key,value in ctx.new_files_db:
			s.write(base64.b64encode(key)+":"+base64.b64encode(value)+"\n")
		s.flush()
		
		# Upload the special data to the containers
		self.container_config.finalize_increment(base_diff)

	def restore(self,target_path):
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.blocks_cache = BlockCache(self)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)

			self.do_restore(target_path)
			self.txn_handler.commit()
		except:
			self.txn_handler.abort()
			traceback.print_exc()
			raise
		finally:
			for key,files_db in self.open_files_dbs.iteritems():
				files_db.close()
			self.blocks_db.close()
			self.blocks_cache.close()
			self.container_config.close()
			self.db_config.close()
	#
	# Functionality for restore mode
	#
	def do_restore(self,target_path):
		#
		# Create the scratch database to precompute block to container requirements
		#
		increment_idx = self.container_config.last_finalized_increment()
		if increment_idx != None:
			files_db = self.__load_files_db(increment_idx)
			increment = self.container_config.increments[increment_idx]
			base_files_db = None
			if increment.base_index != None:
				base_files_db = self.__load_files_db(increment.base_index)
		else:
			raise "No finalized increment found. Nothing to restore"

		ctx = RestoreContext(self,base_files_db,files_db)
		#
		# Compute reference counts for all the blocks required in this restore
		#
		print "1. Computing reference counts"
		self.root = Directory(self,None,target_path)
		self.root.set_num(0)
		self.root.code = Nodes.NODE_DIR
		self.root.request_blocks(ctx,self.blocks_cache)
		# inodes_db must be clean once again when we start restoring the files data
		ctx.inodes_db = {}
		# just to free up resources

		print "2. Computing the list of required blocks of each container"
		self.blocks_cache.analyze()

		#
		# Now restore the files
		#
		print "3. Restoring files"
		self.root.name = target_path
		self.root.restore(ctx)

		print "MAX loaded size:", self.blocks_cache.max_loaded_size

	#
	# Information
	#
	def info(self):
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.blocks_cache = BlockCache(self)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)
			
			self.do_info()
			self.txn_handler.commit()
		except:
			self.txn_handler.abort()
			traceback.print_exc()
			raise
		finally:
			for key,files_db in self.open_files_dbs.iteritems():
				files_db.close()
			self.blocks_db.close()
			self.blocks_cache.close()
			self.container_config.close()
			self.db_config.close()
	def do_info(self):
		
		print "Containers"
		self.container_config.info()
		print "there are %d increments" % len(self.container_config.increments)
		for increment in self.container_config.increments:
			#if not self.database_loaded(i):
				#print "Increment %d not loaded" % i
				#continue
			files_db = self.__load_files_db(increment.index)
			base_files_db = None
			if increment.base_index != None:
				base_files_db = self.__load_files_db(increment.base_index)

			ctx = RestoreContext(self,base_files_db,files_db)
			
			print "Listing of increment %d:" % (increment.index)
			self.root = Directory(self,None,self.data_path)
			self.root.set_num(0)
			self.root.code = Nodes.NODE_DIR
			self.root.list_files(ctx)
	#
	# Files database loading
	#
	def __open_files_db(self,index):
		if not self.open_files_dbs.has_key(index):
			db = self.db_config.get_database(".files.%d"%index,self.txn_handler)
			self.open_files_dbs[index] = db
		return self.open_files_dbs[index]
	def __files_db_loaded(self,index):
		db = self.__open_files_db(index)
		return len(db)>0
	def __load_files_db(self,index):
		db = self.__open_files_db(index)
		if len(db)==0:
			# The database is empty - this means that it must be loaded from the backup
			increment = self.container_config.increments[index]
			increment_blocks = increment.list_specials(Container.CODE_FILES)

			for (idx,block) in increment_blocks:
				self.blocks_cache.request_block(block)

			stream = SpecialIStream(self,increment_blocks)
			expr = re.compile(":")
			while True:
				line = stream.readline()
				line = line.rstrip()
				if len(line)==0:
					break
				(key,value) = expr.split(line)
				#print "Read line from stream: [%s:%s]" %(base64.b64decode(key),value)
				db[base64.b64decode(key)]=base64.b64decode(value)
		return db

class ScanContext:
	def __init__(self,backup,root,base_files_db,prev_files_dbs,new_files_db):
		self.backup = backup
		self.root = root
		self.base_files_db = base_files_db
		self.prev_files_dbs = prev_files_dbs
		self.new_files_db = new_files_db

		self.num = 0
		self.last_container = None
		self.inodes_db = {}

		self.total_nodes = 0
		self.changed_nodes = 0

	def next_num(self):
		result = self.num
		self.num += 1
		return result
	def add_block(self,data,digest):
		if self.backup.blocks_db.has_key(digest):
			return
		(container,index) = self.backup.container_config.add_block(data,digest,Container.CODE_DATA)
		print "  added", base64.b64encode(digest), "to", container, index
		if container != self.last_container:
			self.last_container = container
			# We have finished making a new media.
			# write it to the database
			print "Committing blocks db for container", container
			self.root.flush(self)
			self.backup.txn_handler.commit()

		#
		# The order is extremely important here - the block can be saved
		# (and thus, blocks_db can be updated) only after the previous db
		# is committed. Otherwise, the block ends up written as available
		# in a container that is never finalized.
		#
		block = Block(self.backup,digest)
		block.add_container(container)
		block.save()

class RestoreContext:
	def __init__(self,backup,base_files_db,files_db):
		self.backup = backup
		self.base_files_db = base_files_db
		self.files_db = files_db

		self.inodes_db = {}
