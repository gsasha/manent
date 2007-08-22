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

#from Nodes import Directory
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
		self.digests = []
	def write_block(self,data):
		#print "adding block of code", self.code, "length", len(written)
		digest = Digest.dataDigest(data)
		self.backup.container_config.add_block(data,digest,self.code)
		self.digests.append(digest)
	def get_digests(self):
		return self.digests

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

		#self.open_files_dbs = {}
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
			traceback.print_exc()
			self.txn_handler.abort()
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
			traceback.print_exc()
			self.txn_handler.abort()
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
		print "Reconstructing blocks database from containers %d..%d:" %(0,self.container_config.num_containers()),
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
			self.increments_db = self.db_config.get_database(".increments",self.txn_handler)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)

			blocks_cache = BlockCache(self)
			ctx = ScanContext(self,blocks_cache)
			self.do_scan(ctx)
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			ctx.close()
			self.blocks_db.close()
			self.increments_db.close()
			self.container_config.close()
			self.blocks_cache.close()
			self.db_config.close()
		
	def do_scan(self,ctx):
		#
		# Do the real work of scanning
		#
		self.start_increment(ctx)
		root = Directory(self,None,self.data_path)
		ctx.set_root(root)
		root.code = Nodes.NODE_DIR
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
		#
		# TODO: Save the stats db
		#
		print "Exporting the stats db - implement me"
		s = SpecialOStream(self,Container.CODE_STATS)
		s.flush()
		
		# Upload the special data to the containers
		self.container_config.finalize_increment(base_diff)
		

	def start_increment(self,ctx):
		if not self.increments_db.has_key("increments"):
			self.increments_db["increments"] = ""
			
		increments = Format.deserialize_ints(self.increments_db["increments"])
		increment_idx = increments[-1]+1
		print "Starting increment", increment_idx
		
		finalized_increments = {}
		#
		# Decide on which increment to base this one
		#
		# 1. scan the increments to find out what are the latest
		#    non-finalized ones
		last_finalized_increment = None
		last_scan_base_increments = []
		for idx in self.increments:
			if self.increments_db.has_key["i_%d_finalized"%idx]:
				last_finalized_increment = idx
				last_scan_base_increments = [idx]
				finalized_increments[idx] = True
			else:
				last_scan_base_increments.append(idx)
				finalized_increments[idx] = False
		
		# 1. find out the base_fs: what we base this fs on, to base the diff
		#    when saving. What we use is the base fs's of the last finalized
		#    increment, or the last finalized increment itself.
		increment_bases = []
		if last_finalized_increment != None:
			k = "i_%d_base"%last_finalized_increment
			if self.increments_db.has_key[k]:
				increment_bases=Format.deserialize_ints(self.increments_db[k])
				for idx in increment_bases:
					finalized_increments[idx] = True
			else:
				increment_bases = [last_finalized_increment]
		print "Basing increment on", increment_bases
		ctx.set_fs_base(increment_bases)
		
		# 2. find out the base_scan_fs: what are the fs's we read, to base the
		#    scanning on when scanning. What we use is all the last unfinalized increments
		#    and the last finalized one, if one exists.
		print "Basing scan on", last_scan_base_increments
		ctx.set_scan_base(last_scan_base_increments)
		
		# Make sure all the increment fs's that might be necessary are loaded
		for idx in last_scan_base_increments + increment_bases:
			ctx.load_db(idx)
		ctx.set_fs_base(finalized_increments)
		#
		# Create the new increment
		#
		self.cur_increment = Increment(self, increment_idx)
		self.cur_increment.start()
		ctx.load_db(increment_idx)
		ctx.set_new_fs(increment_idx)
		#
		# Write down the increment to db
		#
		self.increments_db["increments"] = Format.serialize_ints(self.increments)
		first_container = self.storage_config.next_container()
		self.increments_db["i_%d_first_container"%increment_idx] = str(first_container)
	def _finalize_increment(self,context):
		total_change_percent = 0.0
		for idx in context.base_increments:
			change_percent = float(self.increments_db["i_%d_change_percent"%idx])
			total_change_percent += change_percent
		total_change_percent += context.change_percent
		if total_change_percent > 0.5:
			#
			# Do the rebasing!
			#
			
			pass

	def restore(self,target_path):
		try:
			self.blocks_db = self.db_config.get_database(".blocks",self.txn_handler)
			self.blocks_cache = BlockCache(self)
			self.container_config = Container.create_container_config(self.container_type)
			self.container_config.init(self,self.txn_handler,self.container_params)

			self.do_restore(target_path)
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
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
		self.root.request_blocks(ctx)
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
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			for key,files_db in self.open_files_dbs.iteritems():
				files_db.close()
			self.blocks_db.close()
			self.blocks_cache.close()
			self.container_config.close()
			self.db_config.close()
	def do_info(self):
		
		print "there are %d increments" % len(self.container_config.increments)
		for increment in self.container_config.increments:
			#if not self.database_loaded(i):
				#print "Increment %d not loaded" % i
				#continue
			if not increment.is_finalized():
				print "Increment %d is not finalized, no files db!" % (increment.index)
				continue
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
		print "Containers"
		self.container_config.info()
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
			for line in stream:
				line = line.rstrip()
				(key,value) = expr.split(line)
				#print "Read line from stream: [%s:%s]" %(base64.b64decode(key),value)
				db[base64.b64decode(key)]=base64.b64decode(value)
			self.txn_handler.commit()
		return db

#=========================================================
# ContextBase
#=========================================================
class ContextBase:
	def __init__(self,backup):
		self.backup = backup
		self.open_files_dbs = {}
		self.open_stats_dbs = {}

	def close(self):
		for idx,fs_db in self.open_files_dbs.iteritems():
			fs_db.close()
		for idx,st_db in self.open_stats_dbs.iteritems():
			st_db.close()

	#
	# Prev db querying
	#
	def get_stats_db(self,idx):
		return self.open_files_dbs[idx]
	def get_files_db(self,idx):
		return self.open_files_dbs[idx]
	def db_base_level(self,idx):
		if self.base_level.has_key(idx):
			return self.base_level[idx]
		return None
	
	def load_db(self,idx):
		if self.open_files_dbs.has_key(idx):
			return
		files_db = backup.open_files_db(idx)
		stats_db = backup.open_stats_db(idx)
		self.open_files_dbs[idx] = files_db
		self.open_stats_dbs[idx] = stats_db
		if len(files_db) > 0:
			# Databases are loaded. Do nothing more
			return
		# Try to load db data
		self.backup.load_files_db_data(files_db,idx)
		self.backup.load_stats_db_data(stats_db,idx)

#=========================================================
# ScanContext
#=========================================================
class ScanContext(ContextBase):
	def __init__(self,backup,blocks_cache,new_files_db):
		ContextBase.__init__(self,backup)
		self.blocks_cache = blocks_cache
		self.new_files_db = new_files_db

		self.node_num = 0
		self.last_container = None
		self.inodes_db = {}

		self.total_nodes = 0
		self.changed_nodes = 0

	def set_root(self,root):
		self.root = root

	def next_node_num(self):
		result = self.node_num
		self.node_num += 1
		return result
	
	def add_block(self,data,digest):
		if self.backup.blocks_db.has_key(digest):
			return
		(container,index) = self.backup.container_config.add_block(data,digest,Container.CODE_DATA)
		print "  added", base64.b64encode(digest), "to", container, index
		if container != self.last_container:
			self.last_container = container
			# We have finished making a new container.
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

#=========================================================
# RestoreContext
#=========================================================
class RestoreContext:
	def __init__(self,backup,base_files_db,files_db):
		self.backup = backup
		self.base_files_db = base_files_db
		self.files_db = files_db

		self.inodes_db = {}
