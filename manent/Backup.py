#
# TODO:
# 3. Encrypt the contents of containers
#
import sys, os
import base64
from cStringIO import StringIO
import struct
import re

from Config import Config
from Nodes import Directory
import Container
from BlockCache import BlockCache
from Block import Block
from Database import DatabaseConfig
from StreamAdapter import *

class SpecialOStream(OStreamAdapter):
	"""
	This ostream writes its data to a stream of containers
	"""
	def __init__(self,backup,code):
		OStreamAdapter.__init__(self, backup.config.blockSize())
		self.backup = backup
		self.code = code
	def write_block(self,data):
		#print "adding block of code", self.code, "length", len(written)
		digest = self.backup.config.dataDigest(data)
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
		data = self.backup.read_block(digest,idx)
		return data

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self,global_config,label):
		self.global_config = global_config
		self.label = label

		self.db_config = DatabaseConfig(self.global_config)
		self.blocks_db = self.db_config.get_database("manent."+self.label, ".blocks")
		self.inodes_db = {}

	#
	# Three initialization methods:
	# Creation of new Backup, loading from live DB, loading from backups
	#
	def configure(self,data_path,container_type,container_params):
		print "Creating backup", self.label, "type:", container_type, container_params
		self.data_path = data_path
		
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)
		
		self.config = Config()
		self.root = Directory(self,None,self.data_path)

	def load(self,data_path,container_type,container_params):
		print "Loading backup", self.label
		self.data_path = data_path
				
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)
		
		self.config = Config()
		self.root = Directory(self,None,self.data_path)

	def reconstruct(self,data_path,container_type,container_params):
		print "Reconstructing backup", self.label, "type:", container_type, container_params
		self.data_path = data_path

		self.blocks_cache = BlockCache(self)
		
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)

		self.config = Config()
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

	def close(self):
		self.db_config.commit()
		self.db_config.close()
	#
	# Functionality for scan mode
	#
	def scan(self):
		#
		# Check in which increments we should look
		#
		prev_files_dbs = []
		prev_nums = []
		for i in self.container_config.prev_increments():
			if self.files_db_loaded(i):
				prev_files_dbs.append(self.load_files_db(i))
				prev_nums.append((len(prev_nums),0))
		prev_nums.reverse()
		print "Previous increments are: ", prev_nums

		# TODO: base this increment on some previous one, to reduce the size
		# of the generated files db
		
		root = Directory(self,None,self.data_path)
		increment = self.container_config.start_increment()
		new_files_db = self.create_files_db(increment)
		#
		# Do the real work of scanning
		#
		class ScanContext:
			def __init__(self,backup,root,prev_files_dbs,new_files_db):
				self.backup = backup
				self.root = root
				self.prev_files_dbs = prev_files_dbs
				self.new_files_db = new_files_db
				
				self.num = 0
				self.last_container = None
				self.inodes_db = {}

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
					self.backup.db_config.commit()

				#
				# The order is extremely important here - the block can be saved
				# (and thus, blocks_db can be updated) only after the previous db
				# is committed. Otherwise, the block ends up written as available
				# in a container that is never finalized.
				#
				block = Block(self.backup,digest)
				block.add_container(container)
				block.save()
		ctx = ScanContext(self,root,prev_files_dbs,new_files_db)
		root.set_num(ctx.next_num())
		root.scan(ctx,prev_nums)
		
		#
		# Save the files db
		#
		print "Exporting the files db"
		s = SpecialOStream(self,Container.CODE_FILES)
		for key,value in ctx.new_files_db:
			s.write(base64.b64encode(key)+":"+base64.b64encode(value)+"\n")
		s.flush()
		
		# Upload the special data to the containers
		self.container_config.finalize_increment()
		self.db_config.commit()

	#
	# Functionality for restore mode
	#
	def restore(self):
		class RestoreContext:
			def __init__(self,backup,files_db):
				self.backup = backup
				self.files_db = files_db

				self.inodes_db = {}
		#
		# Create the scratch database to precompute block to container requirements
		#
		self.blocks_cache = BlockCache(self)
	
		increment = self.container_config.last_finalized_increment()
		if increment != None:
			files_db = self.load_files_db(increment)
		else:
			raise "No finalized increment found. Nothing to restore"

		ctx = RestoreContext(self,files_db)
		#
		# Compute reference counts for all the blocks required in this restore
		#
		print "1. Computing reference counts"
		self.root.name = "."
		self.root.set_num(0)
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
		self.root.name = "."
		self.root.restore(ctx)

		#self.blocks_cache.close()
		#self.db_config.close()

		print "MAX loaded size:", self.blocks_cache.max_loaded_size

	def read_block(self,digest,index=None):
		"""
		Return the data for the given digest
		"""
		return self.blocks_cache.load_block(digest,index)

	#
	# Information
	#
	def info(self):
		self.blocks_cache = BlockCache(self)
		print "Containers"
		self.container_config.info()

		for i in self.container_config.increments:
			#if not self.database_loaded(i):
				#print "Increment %d not loaded" % i
				#continue
			db = self.load_files_db(i.index)
			print "Listing of increment",i.index
			self.root = Directory(self,None,self.data_path)
			self.root.set_num(0)
			self.root.list_files(db)
			# just to be safe

		#self.db_config.close()
	#
	# Files database loading
	#
	def files_db_loaded(self,increment):
		if not self.db_config.database_exists("manent."+self.label, ".files.%d"%increment):
			return False
		# Consider checking if the DB is empty
		return True
	def create_files_db(self,index):
		db = self.db_config.get_database("manent."+self.label, ".files.%d"%index)
		return db
	def load_files_db(self,index):
		db = self.db_config.get_database("manent."+self.label, ".files.%d"%index)
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
		#self.new_files_db = self.global_config.get_database("manent."+self.label, ".files%d"%(increment),True)
