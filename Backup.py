#
# TODO:
# 3. Encrypt and compress the contents of containers
# 5. Consider changing the container format: have the
#    header and the data in separate files.
#
import sys, os
import base64
from cStringIO import StringIO
import struct

from Config import Config
from Nodes import Directory
import Container

class Block:
	def __init__(self,backup,digest):
		self.backup = backup
		self.digest = digest

		self.containers = []
		if not self.backup.blocks_db.has_key(self.digest):
			return
		data = self.backup.blocks_db[self.digest]
		blockFile = StringIO(data)
		while True:
			containerNum = self.backup.config.read_int(blockFile)
			if containerNum == None:
				break
			self.containers.append(containerNum)
	def add_container(self,container):
		self.containers.append(container)
	def save(self):
		result = StringIO()
		for container in self.containers:
			self.backup.config.write_int(result,container)
		self.backup.blocks_db[self.digest] = result.getvalue()
	
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
		# TODO: This is a hack that it sits here. There should be a general close
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

		self.container_config = Container.create_container_config(container_type)
		self.container_config.init(self,container_params)

		self.config = Config()
		self.root = Directory(None,self.data_path)

		#
		# Reconstruct the containers dbs
		#
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
			self.prev_files_dbs.append(self.global_config.get_database("manent."+self.label, ".files%d"%(i), True))
			prev_nums.append((len(prev_nums),0))
		prev_nums.reverse()
		self.new_files_db = self.global_config.get_database("manent."+self.label, ".files%d"%(increment),True)
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
		self.scratch_db = self.global_config.get_database("manent."+self.label,".scratch_blocks",True)
		# If a previous removal was terminated in the middle, scratch db will
		# contain junk. Clean it up just in that case.
		self.scratch_db.truncate()
		
		increment = self.container_config.last_finalized_increment()
		if increment != None:
			self.files_db = self.global_config.get_database("manent."+self.label,".files%d"%(increment),True)
		else:
			raise "No finalized increment found. Nothing to restore"

		#
		# Compute reference counts for all the blocks required in this restore
		#
		print "1. Computing reference counts"
		self.root.name = "."
		self.root.count_blocks(self,0,self.scratch_db)
		# just to free up resources
		self.scratch_db.commit()

		print "2. Computing the list of required blocks of each container"
		self.scratch_container_db = {}
		for key,count in self.scratch_db:
			block = Block(self,key)
			container_index = block.containers[0]
			for container_index in block.containers:
				if self.scratch_container_db.has_key(container_index):
					self.scratch_container_db[container_index] = self.scratch_container_db[container_index]+int(count)
				else:
					self.scratch_container_db[container_index] = int(count)
		for key,count in self.scratch_container_db.items():
			print "  ", key, "\t-->", count

		self.loaded_blocks = {}
		self.loaded_size = 0
		self.max_loaded_size = 0

		#
		# Now restore the files
		#
		print "3. Restoring files"
		self.root.name = "."
		self.root.restore(self,0)
		
		self.blocks_db.close()
		self.files_db.close()
		self.container_config.close()

		self.scratch_db.close()
		self.scratch_db.remove()

		print "MAX loaded size:", self.max_loaded_size

	def read_block(self,digest):
		"""
		Return the data for the given digest
		"""
		#print "Reading block", base64.b64encode(digest)
		block = Block(self,digest)
		#
		# Block is not found. Decide which container to load for it.
		#
		if not self.loaded_blocks.has_key(digest):
			candidates = [(self.scratch_container_db[x],x) for x in block.containers]
			candidates.sort()
			#print "Candidate containers", candidates
			container_idx = candidates[-1][1] # select the container with max refcount
			#print "loading block from container", container_idx
			container = self.container_config.get_container(container_idx)
			self.container_config.load_container_data(container_idx)
			report = container.read_blocks(self.scratch_db)
			for (d, size) in report.items():
				if self.loaded_blocks.has_key(d):
					# We currently don't handle it, but generally, it's a good idea
					# to make sure that the container uploads only those blocks that
					# are not in the cache
					print "AARRRGGGHHH! FIX ME!"
					continue
				self.loaded_blocks[d] = size
				self.loaded_size += size
				if self.loaded_size > self.max_loaded_size:
					self.max_loaded_size = self.loaded_size
		#
		# Load the block data
		#
		block_file_name = self.config.block_file_name(digest)
		block_file = open(block_file_name, "r")
		block_data = block_file.read()
		block_file.close()
		#
		# Update the refcounts, delete the block if no longer necessary
		#
		self.scratch_db[digest] = str(int(self.scratch_db[digest])-1)
		if self.scratch_db[digest] == "0":
			#print "Block", base64.urlsafe_b64encode(digest), "is not used anymore, removing!"
			os.unlink(block_file_name)
			self.loaded_size -= len(block_data)
		for container in block.containers:
			self.scratch_container_db[container] -= 1
		#
		# Ok, done
		#
		return block_data

		#block = Block(self,digest)
		#container_index = block.containers[0]
		#container = self.container_config.load_container(container_index)
		#return container.read_block(digest)

	#
	# Information
	#
	def info(self):
		print "Containers"
		self.container_config.info()
		prev_increments = self.container_config.prev_increments()
		for i in prev_increments:
			db = self.global_config.get_database("manent."+self.label, ".files%d"%(i), True)
			print "Listing of increment",i
			self.root = Directory(None,self.data_path)
			self.root.list_files(self,0,db)
			# just to be safe
			db.close()
			db = None
		# Just to make the db happy
		self.blocks_db.close()
		self.container_config.close()
