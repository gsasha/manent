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
	
class SpecialOStream:
	def __init__(self,backup,code):
		self.backup = backup
		self.code = code
		self.chunk = self.backup.config.blockSize()
		self.buf = StringIO()
		self.buflen = 0
		self.total = 0
	def write(self,data):
		self.buf.write(data)
		self.buflen += len(data)
		self.total += len(data)
		while self.buflen > self.chunk:
			self.write_chunk()
	def flush(self):
		#print "Special block",self.code,"total size",self.total
		while self.buflen > 0:
			self.write_chunk()
	def write_chunk(self):
		chunk = self.buflen
		if chunk > self.chunk:
			chunk = self.chunk
		buf = self.buf.getvalue()
		written = buf[0:chunk]
		self.buflen -= chunk
		if self.buflen > 0:
			self.buf = StringIO()
			self.buf.write(buf[chunk:])
		#print "adding block of code", self.code, "length", len(written)
		digest = self.backup.config.dataDigest(written)
		self.backup.container_config.add_block(written,digest,self.code)

class SpecialIStream:
	def __init__(self,backup,digests):
		self.backup = backup
		self.digests = digests

		self.buf = None
	def read(self,size):
		result = ""
		while len(result) < size:
			if (self.buf == None):
				chunk = self.read_chunk()
				if len(chunk) == 0:
					break
				self.buf = StringIO(chunk)
			data = self.buf.read(size-len(result))
			if len(data) == 0:
				self.buf = None
				continue
			result += data
		return result
	def readline(self):
		result = StringIO()
		while True:
			ch = self.read(1)
			if len(ch) == 0:
				break
			result.write(ch)
			if ch == "\n":
				break
		return result.getvalue()
	def read_chunk(self):
		if len(self.digests)==0:
			return ""
		(idx,digest) = self.digests[0]
		self.digests = self.digests[1:]
		chunk = self.backup.read_block(digest,idx)
		return chunk

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self,global_config,label):
		self.global_config = global_config
		self.label = label

		self.blocks_db = self.global_config.get_database("manent."+self.label, ".blocks",True)
		self.inodes_db = {}
		
	def configure(self,data_path,container_type,container_params):
		print "Creating backup", self.label, "type:", container_type, container_params
		self.data_path = data_path
		
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)
		
		self.config = Config()
		self.root = Directory(None,self.data_path)

		#
		# TODO: This is a hack that these calls sit here. There should be a general close()
		#       method that would operate for all operations, like load etc.
		#
		self.blocks_db.commit()
		self.blocks_db.close()
		self.container_config.commit()
		self.container_config.close()

	def load(self,data_path,container_type,container_params):
		print "Loading backup", self.label
		self.data_path = data_path
				
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)
		
		self.config = Config()
		self.root = Directory(None,self.data_path)

	def reconstruct(self,data_path,container_type,container_params):
		print "Reconstructing backup", self.label, "type:", container_type, container_params
		self.data_path = data_path

		self.blocks_cache = BlockCache(self)
		
		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)

		self.config = Config()
		self.root = Directory(None,self.data_path)

		#
		# Reconstruct the containers dbs
		#
		print "Reconstructing container config"
		self.container_config.reconstruct()
		#
		# Reconstruct the blocks db
		#
		print "Reconstructiong blocks database:",
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

		self.blocks_cache.close()
		self.blocks_db.commit()
		self.blocks_db.close()
		self.container_config.commit()
		self.container_config.close()
	#
	# Functionality for scan mode
	#
	def scan(self):
		self.last_container = None
		self.root = Directory(None,self.data_path)
		increment = self.container_config.start_increment()
		prev_increments = self.container_config.prev_increments()
		print "Previous increments", prev_increments
		self.prev_files_dbs = []
		prev_nums = []
		for i in prev_increments:
			if self.files_db_loaded(i):
				self.prev_files_dbs.append(self.load_files_db(i))
				prev_nums.append((len(prev_nums),0))
		prev_nums.reverse()
		self.new_files_db = self.create_files_db(increment)
		#self.new_files_db = self.global_config.get_database("manent."+self.label, ".files%d"%(increment),True)
		#
		# Do the real work of scanning
		#
		self.root.scan(self,0,prev_nums)
		
		#
		# Save all the results
		#
		self.blocks_db.commit()
		self.new_files_db.commit()
		# Types of special data:
		# 1 - Filesystem database
		# 2 - Blocks database
		print "Exporting the files db"
		os = SpecialOStream(self,Container.CODE_FILES)
		for key,value in self.new_files_db:
			os.write(base64.b64encode(key)+":"+base64.b64encode(value)+"\n")
		os.flush()
		
		# Upload the special data to the containers
		self.container_config.finalize_increment()
		self.container_config.commit()
		
		#
		# Avoid warning on implicitly closed DB
		#
		for db in self.prev_files_dbs:
			db.close()
		self.blocks_db.close()
		self.new_files_db.close()
		self.container_config.close()
	def add_block(self,data,digest):
		if self.blocks_db.has_key(digest):
			return
		(container,index) = self.container_config.add_block(data,digest,Container.CODE_DATA)
		print "  adding", base64.b64encode(digest), "to", container, index
		if container != self.last_container:
			self.last_container = container
			# We have finished making a new media.
			# write it to the database
			print "Committing blocks db for container", container
			self.blocks_db.commit()
			self.new_files_db.commit()
			self.container_config.commit()
		
		block = Block(self,digest)
		block.add_container(container)
		block.save()

	#
	# Functionality for restore mode
	#
	def restore(self):
		#
		# Create the scratch database to precompute block to container requirements
		#
		self.blocks_cache = BlockCache(self)
	
		increment = self.container_config.last_finalized_increment()
		if increment != None:
			self.files_db = self.load_files_db(increment)
		else:
			raise "No finalized increment found. Nothing to restore"

		#
		# Compute reference counts for all the blocks required in this restore
		#
		print "1. Computing reference counts"
		self.root.name = "."
		self.root.request_blocks(self,0,self.blocks_cache)
		# just to free up resources

		print "2. Computing the list of required blocks of each container"
		self.blocks_cache.analyze()

		#
		# Now restore the files
		#
		print "3. Restoring files"
		self.root.name = "."
		self.root.restore(self,0)
		
		self.blocks_db.close()
		self.files_db.close()
		self.container_config.close()

		self.blocks_cache.close()

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
		prev_increments = self.container_config.prev_increments()
		for i in prev_increments:
			#if not self.database_loaded(i):
				#print "Increment %d not loaded" % i
				#continue
			db = self.load_files_db(i)
			print "Listing of increment",i
			self.root = Directory(None,self.data_path)
			self.root.list_files(self,0,db)
			# just to be safe
			db.close()
			db = None
		# Just to make the db happy

		self.blocks_cache.close()
		self.blocks_db.close()
		self.container_config.close()
	#
	# Files database loading
	#
	def files_db_loaded(self,increment):
		if not self.global_config.database_exists("manent."+self.label, ".files.%d"%increment):
			return False
		# Consider checking if the DB is empty
		return True
	def create_files_db(self,index):
		db = self.global_config.get_database("manent."+self.label, ".files.%d"%index, True)
		return db
	def load_files_db(self,index):
		db = self.global_config.get_database("manent."+self.label, ".files.%d"%index, True)
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
			db.commit()
		return db
		#self.new_files_db = self.global_config.get_database("manent."+self.label, ".files%d"%(increment),True)
