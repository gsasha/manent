from cStringIO import StringIO
import os, os.path, shutil
import re
import base64
import bz2
import traceback
import sys

import manent.utils.Digest as Digest
import manent.utils.Format as Format
from Increment import Increment
from StreamAdapter import *
from manent.utils.BandwidthLimiter import *
from manent.utils.RemoteFSHandler import FTPHandler, SFTPHandler
from manent.utils.FileIO import FileReader, FileWriter

compression_index = -1
compression_root = os.getenv("MANENT_COMPRESSION_LOG_ROOT")

def create_container_config(containerType):
	if containerType == "directory":
		return DirectoryContainerConfig()
	elif containerType == "mail":
		return MailContainerConfig()
	elif containerType == "ftp":
		return FTPContainerConfig(FTPHandler)
	elif containerType == "sftp":
		return FTPContainerConfig(SFTPHandler)
	#elif containerType == "gmail":
	#	return GmailContainerConfig()
	#elif containerType == "optical":
	#	return OpticalContainerConfig()
	raise "Unknown container type", containerType

class NoopCompressor:
	def compress(self,data):
		return data
	def flush(self):
		return ""

#
# Codes for the different block types
#
CODE_DATA                  =  0
CODE_FILES                 =  1

CODE_CONTAINER_START       = 32
CODE_INCREMENT_START       = 33
CODE_INCREMENT_END         = 34

CODE_COMPRESSION_END       = 48
CODE_COMPRESSION_BZ2_START = 49
CODE_COMPRESSION_GZIP_START= 50

class Container:
	"""
	Represents one contiguous container that can be saved somewhere, i.e.,
	on an optical disk, in a mail system, over the network etc.

	Container consists in one of two states:
	1. Normal - in this state, blocks are added to the container
	2. Frozen - in this state, the container is completed. It can be written
	            out, or its blocks can be read back.

	Container can have its contents encrypted - TODO
	Container can have its contents compressed - TODO

	The container data consists of blocks that contain the following data:
	(digest, size, code). The code can be 0 (regular data) or special data.
	The aim of "size" for special code can carry a different meaning: for instance,
	for compression, it is the offset in the data file itself, so that scanning can be
	restarted there.
	
	CODE_CONTAINER_START: Must be the first one in a container. Contains a human-
	readable description of the container
	CODE_INCREMENT_START: The descriptor of an increment. Contains machine-readable data
	used for backup rescanning
	CODE_INCREMENT_END: same
	CODE_FILES: Special descriptors used for Backup
	"""
	def __init__(self,backup,index):
		# Configuration data
		self.backup = backup
		self.index = index
		self.mode = None
		self.frozen = False
		self.compressor = None

	#
	# Utility functions
	#
	def filename(self):
		return self.backup.global_config.container_file_name(self.backup.label, self.index)
	def __repr__(self):
		return "Container totalsize=%d, %d blocks" % (self.totalSize, len(self.blocks))

	#
	# Dumping mode implementation
	#
	def start_dump(self):
		self.mode = "DUMP"
		self.totalSize = 0
		self.compressionSize = 0
		# Each block will contain a tuple of (digest, size, code)
		self.blocks = []
		self.incrementBlocks = []
		self.dataFileName = os.path.join(self.backup.global_config.staging_area(),self.filename()+".data")
		try:
			os.unlink(self.dataFileName)
		except:
			#it's OK if the file does not exist
			pass
		self.dataFile = open(self.dataFileName, "wb")

		# Add the "container start" special block
		message = "Manent container #%d of backup '%s'\n\0x1b" % (self.index, self.backup.label)
		self.dataFile.write(message)
		self.totalSize += len(message)
		self.blocks.append((Digest.dataDigest(message),len(message),CODE_CONTAINER_START))

		self.start_compression()
		
	def start_compression(self):
		self.finish_compression(restart=True)
		self.blocks.append((Digest.dataDigest(""),self.totalSize,CODE_COMPRESSION_BZ2_START))
		self.compressor = bz2.BZ2Compressor(9)
		self.compressedSize = 0
		self.uncompressedSize = 0

		try:
			global compression_root
			if compression_root != None:
				global compression_index
				compression_index += 1
				os.mkdir("%s/compress-%04d"%(compression_root,compression_index))
				self.compression_block_index = 0
		except:
			pass
	def finish_compression(self,restart=False):
		if self.compressor == None:
			return
		remainder = self.compressor.flush()
		self.dataFile.write(remainder)
		self.totalSize += len(remainder)
		self.compressedSize += len(remainder)
		if not restart:
			self.blocks.append((Digest.dataDigest(""),self.totalSize,CODE_COMPRESSION_END))
		#print "Compressed remainder of size", len(remainder)
		#print "Total compressed  ", self.compressedSize, self.totalSize
		#print "Total uncompressed", self.uncompressedSize
		self.compressor = None
	def can_append(self,data):
		# TODO: Take into account the size of the data table, which would be relevant for
		#       very small files
		current_size = self.totalSize + 64*len(self.blocks)
		return current_size+len(data) <= self.backup.container_config.container_size()
	def append(self,data,digest,code):
		if (code==CODE_INCREMENT_START) or (code==CODE_INCREMENT_END):
			#
			# Increment start and increment end blocks are stored in the
			# header file, in addition to the data file
			#
			self.incrementBlocks.append((data,digest,code))
		if self.compressor:
			try:
				global compression_root
				if compression_root != None:
					global compression_index
					of_name = "%s/compress-%04d/block.%04d" % (compression_root,compression_index, self.compression_block_index)
					print "Writing file", of_name
					of = open(of_name, "w")
					of.write(data)
					of.close()
					self.compression_block_index+=1
			except:
				pass
			compressed = self.compressor.compress(data)

			#print "Compressed data from", len(data), "to", len(compressed)
			self.compressionSize += len(compressed)
			self.compressedSize += len(compressed)
			self.totalSize += len(compressed)
			self.uncompressedSize += len(data)
			self.blocks.append((digest,len(data),code))
			self.dataFile.write(compressed)
			if self.compressionSize > self.backup.container_config.compression_block_size():
				self.start_compression()
				self.compressionSize = 0
		else:
			self.blocks.append((digest,len(data),code))
			self.dataFile.write(data)
		#self.totalSize += len(compressed)
		return len(self.blocks)
	
	def finish_dump(self):
		self.finish_compression(restart=False)
		filepath = os.path.join(self.backup.global_config.staging_area(),self.filename())
		try:
			os.unlink(filepath)
		except:
			# do nothing! We don't really expect the file to be there
			pass
		
		file = open(filepath, "wb")
		#
		# Write the header
		#
		MAGIC = "MNNT"
		VERSION = 1

		file.write(MAGIC)
		Format.write_int(file,VERSION)
		Format.write_int(file,self.index)

		table = StringIO()
		Format.write_int(table, len(self.blocks))
		for (digest,size,code) in self.blocks:
			Format.write_int(table,size)
			Format.write_int(table,code)
			table.write(digest)

		tableContents = table.getvalue()
		table.close()
		Format.write_int(file,len(tableContents))
		file.write(Digest.headerDigest(tableContents))
		file.write(tableContents)
		#
		# Store the increment blocks in the file
		#
		for (data,digest,code) in self.incrementBlocks:
			file.write(data)
		#
		# Copy the contents from the data file to the container file
		#
		print "Closing datafile", self.dataFileName
		self.dataFile.close()
		# Do the testing
		self.test_blocks(self.dataFileName)
		self.dataFileName = None
	def isempty(self):
		return len(self.blocks) == 0
	def numblocks(self):
		return len(self.blocks)
	#
	# Loading mode implementation
	#
	def load(self,filename=None):
		if self.mode == "LOAD":
			return
		
		self.mode = "LOAD"
		self.totalSize = 0
		self.blocks = []
		self.block_infos = {}

		if filename == None:
			filename = os.path.join(self.backup.global_config.staging_area(),self.filename())
		file = open(filename, "rb")
		MAGIC = file.read(4)
		if MAGIC != "MNNT":
			raise "Manent: magic didn't happen..."
		VERSION = Format.read_int(file)
		index = Format.read_int(file)
		if index != self.index:
			raise "Manent: wrong index of container file. Expected %s, found %s" % (str(self.index),str(index))
		
		tableBytes = Format.read_int(file)
		tableDigest = file.read(Digest.headerDigestSize())
		tableContents = file.read(tableBytes)
		if Digest.headerDigest(tableContents) != tableDigest:
			raise "Manent: header of container file corrupted"
		tableFile = StringIO(tableContents)
		for i in range(0,Format.read_int(tableFile)):
			blockSize = Format.read_int(tableFile)
			blockCode = Format.read_int(tableFile)
			blockDigest = tableFile.read(Digest.dataDigestSize())
			self.blocks.append((blockDigest,blockSize,blockCode))
		self.indexBlocks = []
		for (digest,size,code) in self.blocks:
			if (code==CODE_INCREMENT_START) or (code==CODE_INCREMENT_END):
				# We assume that the blocks are just appended there
				data = file.read(size)
				if Digest.dataDigest(data) != digest:
					raise "Manent: index block corrupted"
				self.indexBlocks.append((data,digest,code))
	def info(self):
		print "Manent container #%d of backup %s" % (self.index, self.backup.label)
		for (digest,size,code) in self.blocks:
			if code == CODE_DATA:
				print " DATA  [%6d] %s" % (size, base64.b64encode(digest))
			elif code == CODE_FILES:
				print " FILES [%6d] %s" % (size, base64.b64encode(digest))
			elif code == CODE_CONTAINER_START:
				print " CONTAINER_START [%6d]" % (size)
			elif code == CODE_COMPRESSION_END:
				print " COMPRESSION END AT %d" % (size)
			elif code == CODE_COMPRESSION_BZ2_START:
				print " BZ2 START AT %d" % (size)
			elif code == CODE_COMPRESSION_GZIP_START:
				print " GZIP START AT %d" % (size)
			elif code == CODE_INCREMENT_START:
				print " INCREMENT START [%6d]" % (size)
			elif code == CODE_INCREMENT_END:
				print " INCREMENT END [%6d]" % (size)
	def find_increment_start(self):
		for (digest,size,code) in self.blocks:
			if code == CODE_INCREMENT_START:
				return self.get_index_block(digest)
		return None
	def find_increment_end(self):
		for (digest,size,code) in self.blocks:
			if code == CODE_INCREMENT_END:
				return self.get_index_block(digest)
		return None
	def get_index_block(self,digest):
		for (blockData,blockDigest,blockCode) in self.indexBlocks:
			if blockDigest==digest:
				return blockData
		raise "Requested index block not found"
	def test_blocks(self,filename=None):
		class TestingBlockCache:
			def __init__(self):
				pass
			def block_needed(self,digest):
				return True
			def block_loaded(self,digest,block):
				new_digest = Digest.dataDigest(block)
				if new_digest != digest:
					raise "Critical error: Bad digest in container!"
		bc = TestingBlockCache()
		self.read_blocks(bc,filename)
	def read_blocks(self,block_cache,filename = None):
		compression_start_offset = None
		block_offset = 0
		last_read_offset = 0
		decompressor = None
		print "Unpacking container", self.index
		if filename == None:
			filename = os.path.join(self.backup.global_config.staging_area(),self.filename()+".data")
		file = open(filename, "rb")
		#
		# Compute compression block sizes. This is necessary because
		# we cannot give extraneous data to decompressor
		#
		compression_sizes = {}
		last_compression = None
		for (digest,size,code) in self.blocks:
			if code == CODE_COMPRESSION_END:
				if last_compression == None:
					raise "OOPS: Compression end tag without corresponding start"
				compression_sizes[last_compression] = size-last_compression
				last_compression = None
			if code == CODE_COMPRESSION_BZ2_START:
				#print "See compression start bz2"
				if last_compression != None:
					compression_sizes[last_compression] = size-last_compression
				last_compression = size
		if last_compression != None:
			# No need to limit - the container won't give more data by itself
			compression_sizes[last_compression] = None

		for (digest,size,code) in self.blocks:
			if code == CODE_COMPRESSION_END:
				decompressor = None
			elif code == CODE_COMPRESSION_BZ2_START:
				compression_start_offset = size
				block_offset = 0
				last_read_offset = 0
				file.seek(size)
				#print "Starting bzip2 decompressor"
				decompressor = BZ2FileDecompressor(file,compression_sizes[size])
			elif code == CODE_COMPRESSION_GZIP_START:
				compression_start_offset = size
				block_offset = 0
				last_read_offset = 0
				file.seek(size)
				decompressor = GZIPFileDecompressor(file,compression_sizes[size])
			elif block_cache.block_needed(digest):
				#print "Loading block", base64.b64encode(digest), ", size", size, ",rc=", digest_db[digest]
				if decompressor != None:
					# No compression applied. Can just read the block.
					source = decompressor
				else:
					source = file

				#print "block_offset %d, last_read_offset %d" % (block_offset, last_read_offset)
				source.seek(block_offset-last_read_offset,1)
				last_read_offset = block_offset+size
				block = source.read(size)
				# check that the block is OK
				blockDigest = Digest.dataDigest(block)
				if (digest != blockDigest):
					raise "OUCH!!! The block read is incorrect: expected %s, got %s" % (str(base64.b64encode(digest)), str(base64.b64encode(blockDigest)))

				#
				# write the block out
				#
				block_cache.block_loaded(digest,block)
				block_offset += size
			
			else:
				# All the blocks except CODE_COMPRESSION_BZ2_START use size for really size
				block_offset += size


class BZ2FileDecompressor(IStreamAdapter):
	def __init__(self,file,limit):
		IStreamAdapter.__init__(self)
		
		self.file = file
		self.limit = limit
		self.decompressor = bz2.BZ2Decompressor()
	def read_block(self):
		step = 8192
		while True:
			if self.limit == None:
				data = self.file.read(step)
			elif self.limit < step:
				data = self.file.read(self.limit)
			else:
				data = self.file.read(step)
			if len(data) == 0:
				return ""
			decomp = self.decompressor.decompress(data)
			if len(decomp) > 0:
				return decomp

class GZIPFileDecompressor(IStreamAdapter):
	def __init__(self,file,limit):
		IStreamAdapter.__init__(self)
		
		self.file = file
		self.limit = limit
		# TODO: support gzip algorithm here
		self.decompressor = None
	def read_block(self):
		step = 8192
		while True:
			if self.limit == None:
				data = self.file.read(step)
			elif self.limit < step:
				data = self.file.read(self.limit)
			else:
				data = self.file.read(step)
			if len(data) == 0:
				return ""
			decomp = self.decompressor.decompress(data)
			if len(decomp) > 0:
				return decomp
	
class ContainerConfig:
	def __init__(self):
		self.new_increment = None
		self.containers_db = None
	
	#
	# Loading
	#
	def init(self,backup,txn_handler):
		self.backup = backup
		self.containers_db = self.backup.db_config.get_database(".history",txn_handler)

		#
		# See if we are loading the db for the first time
		#
		if not self.containers_db.has_key("Increments"):
			self.containers_db["Increments"] = str(0)
			self.containers_db["Containers"] = str(0)

		#
		# Load the existing increments and containers
		#
		self.containers = []
		for i in range(0,int(self.containers_db["Containers"])):
			self.containers.append(None)
		self.increments = []
		for i in range(0,int(self.containers_db["Increments"])):
			increment = Increment(self,i)
			increment.load()
			self.increments.append(increment)
	def close(self):
		if self.containers_db is not None:
			# It can be none if its initialization fails
			self.containers_db.close()
	def info(self):
		print "Containers:", len(self.containers)
		print "Increments:"
		for increment in self.increments:
			print "  ", increment.index,
			if increment.finalized:
				print "F",
			else:
				print " ",
			print increment.containers,
			print ", base:", increment.base_index, "diff:", increment.base_diff
			
		for i in range(0,len(self.containers)):
			container = self.get_container(i)
			container.info()
			self.containers[i] = None
	#
	# Increment management
	#
	def start_increment(self,base_index):
		if self.new_increment != None:
			raise "Attempting to start an increment before existing one is finalized"
		self.new_increment = Increment(self, int(self.containers_db["Increments"]))
		self.new_increment.start(base_index)
		self.increments.append(self.new_increment)
		self.containers_db["Increments"] = str(int(self.containers_db["Increments"])+1)

		message = self.new_increment.message()
		self.add_block(message,Digest.dataDigest(message),CODE_INCREMENT_START)
		return self.new_increment.index
	def finalize_increment(self,base_diff):
		if self.new_increment == None:
			raise "Attempting to finalized an increment but none was started"
		# TODO: Make the message include the same data as of the starting increment, so that
		#       they can be matched at recovery
		message = self.new_increment.message()
		self.add_block(message,Digest.dataDigest(message),CODE_INCREMENT_END)
		
		if len(self.containers)>0 and self.containers[-1] != None:
			self.containers[-1].finish_dump()
			self.save_container(self.containers[-1])
			self.new_increment.add_container(self.containers[-1].index)
			self.containers_db["Containers"] = str(len(self.containers))

		self.new_increment.finalize(base_diff)
		self.new_increment = None
	#
	# Utility methods for increment management
	#
	def last_finalized_increment(self):
		finalized_increments = [i for i in self.increments if i.finalized]
		if len(finalized_increments) == 0:
			return None
		return finalized_increments[-1].index
	def prev_increments(self):
		"""
		Compute the set of all the last increments, starting from
		the latest finalized one
		"""
		prev_increments = []
		for increment in self.increments:
			if increment.finalized:
				prev_increments = [increment.index]
			else:
				prev_increments.append(increment.index)
		return prev_increments
	def restore_increment(self, start_message, end_message, start_container, end_container, is_finalized):
		increment = Increment(self, int(self.containers_db["Increments"]))
		self.increments.append(self.new_increment)
		self.containers_db["Increments"] = str(int(self.containers_db["Increments"])+1)

		print "Restoring increment", start_message, end_message
		if start_message != end_message:
			is_finalized = False
		increment.restore(start_message, start_container, end_container, is_finalized)
	#
	# Reconstruction!
	#
	def reconstruct(self):
		#
		# It is the specific implementation of ContainerConfig that knows how to reconstruct the
		# containers
		#
		self.reconstruct_containers()
		#
		# TODO: Support cases where some of the containers are lost
		# or broken - in these case, do the best effort, i.e., recover
		# everything that is recoverable. In particular, in cases of redundancy,
		# when everything is recoverable, make sure we do it.
		#
		print "Scanning increments:"
		last_start_container = None
		last_start_message = None
		for container in self.containers:
			if container == None:
				continue
			start_message = container.find_increment_start()
			end_message = container.find_increment_end()
			if start_message != None:
				if last_start_container != None:
					# This is an unfinished increment
					# Create that increment, and start this one
					end_container = container.index-1
					self.restore_increment(last_start_message, end_message, last_start_container, end_container, False)
				if end_message == None:
					last_start_container = container.index
					last_start_message = start_message
			if end_message != None:
				# Found a finished increment
				if start_message != None:
					start_container = container.index
				elif last_start_container != None:
					start_container = last_start_container
					start_message = last_start_message
				else:
					print "Found increment end in container %d, but no previous increment start" % container.index
					continue
				# Create the increment
				end_container = container.index
				print "Found a finished increment in containers %d-%d" % (start_container,end_container)
				self.restore_increment(start_message, end_message, start_container, end_container, True)
				last_start_container = None
	#
	# Container management
	#
	def num_containers(self):
		return len(self.containers)
	def get_container(self,index):
		if self.containers[index] == None:
			self.containers[index] = self.load_container(index)
		return self.containers[index]
	def release_container(self,index):
		self.containers[index] = None
	def add_container(self):
		if len(self.containers)>0 and self.containers[-1] != None:
			self.containers[-1].finish_dump()
			self.save_container(self.containers[-1])
			self.new_increment.add_container(self.containers[-1].index)
			self.containers_db["Containers"] = str(len(self.containers))
		container = Container(self.backup,len(self.containers))
		container.start_dump()
		self.containers.append(container)
		return container
	#
	# Block management
	#
	def add_block(self,data,digest,code):
		if len(self.containers)==0:
			container = self.add_container()
		else:
			container = self.containers[-1]
			if container==None or container.frozen or (not container.can_append(data)):
				container = self.add_container()
		index = container.append(data,digest,code)
		return (container.index, index)

	#
	# Block parameters
	#
	def blockSize(self):
		return 256*1024
	
	def compression_block_size(self):
		return 2*1024*1024


from ftplib import FTP

class FTPContainerConfig(ContainerConfig):
	"""
	Handler for a FTP site storage
	"""
	def __init__(self,RemoteHandlerClass):
		ContainerConfig.__init__(self)
		self.RemoteHandlerClass = RemoteHandlerClass
		self.up_bw_limiter = BandwidthLimiter(15.0E3)
		self.down_bw_limiter = BandwidthLimiter(100.0E3)
	def init(self,backup,txn_handler,params):
		ContainerConfig.init(self,backup,txn_handler)
		(host,user,password,path) = params
		self.fs_handler = self.RemoteHandlerClass(host,user,password,path)
	def container_size(self):
		return 4<<20
	def load_container(self,index):
		print "Loading header for container", index
		container = Container(self.backup,index)
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.download(FileWriter(staging_path,self.down_bw_limiter), filename)
		container.load(staging_path)
		return container
	
	def load_container_data(self,index):
		print "Loading data for container", index
		container = Container(self.backup,index)
		filename = container.filename()+".data"
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.download(FileWriter(staging_path,self.down_bw_limiter), filename)
		return staging_path
	
	def save_container(self,container):
		index = container.index
		
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.upload(FileReader(staging_path,self.up_bw_limiter), filename)
		self.fs_handler.upload(FileReader(staging_path+".data",self.up_bw_limiter), filename+".data")
		os.unlink(staging_path)
		os.unlink(staging_path+".data")
	def reconstruct_containers(self):
		print "Scanning containers:"
		container_files = {}
		container_data_files = {}
		file_list = self.fs_handler.list_files()
		print "listed files", file_list
		for file in file_list:
			container_index = self.backup.global_config.container_index(file,self.backup.label,"")
			if container_index != None:
				container_files[container_index] = file
			container_index = self.backup.global_config.container_index(file,self.backup.label,".data")
			if container_index != None:
				container_data_files[container_index] = file
		max_container = 0
		for (index, file) in container_files.iteritems():
			print "  ", index, "\t", file,
			if container_data_files.has_key(index):
				print "\t", container_data_files[index]
			else:
				print
			if max_container<index:
				max_container = index
		for index in range(0,max_container+1):
			self.containers.append(None)
		self.containers_db["Containers"] = str(len(self.containers))
		
		print "Loading %d containers:" % max_container
		for (index, file) in container_files.iteritems():
			if not container_data_files.has_key(index):
				print "Container", index, "has no data file :("
				continue
			container = Container(self.backup,index)
			self.load_container(index)
			container.load()
			self.containers[index] = container


class DirectoryContainerConfig(ContainerConfig):
	"""
	Handler for a simple directory.
	"""
	def __init__(self):
		ContainerConfig.__init__(self)
		self.path = None
	def init(self,backup,txn_handler,params):
		ContainerConfig.init(self,backup,txn_handler)
		(path,) = params
		self.path = path
	def container_size(self):
		#return 4<<20
		return 4<<20
       	def reconstruct_containers(self):
		print "Scanning containers:", self.path
		container_files = {}
		container_data_files = {}
		for file in os.listdir(self.path):
			container_index = self.backup.global_config.container_index(file,self.backup.label,"")
			if container_index != None:
				container_files[container_index] = file
			container_index = self.backup.global_config.container_index(file,self.backup.label,".data")
			if container_index != None:
				container_data_files[container_index] = file
		max_container = 0
		for (index, file) in container_files.iteritems():
			print "  ", index, "\t", file,
			if container_data_files.has_key(index):
				print "\t", container_data_files[index]
			else:
				print
			if max_container<index:
				max_container = index
		for index in range(0,max_container+1):
			self.containers.append(None)
		self.containers_db["Containers"] = str(len(self.containers))
		
		print "Loading %d containers:" % max_container
		for (index, file) in container_files.iteritems():
			if not container_data_files.has_key(index):
				print "Container", index, "has no data file :("
				continue
			container = self.load_container(index)
			self.containers[index] = container
	def load_container(self,index):
		print "Loading header for container", index
		container = Container(self.backup,index)

		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		container.load(os.path.join(self.path,filename))
		return container
	
	def load_container_data(self,index):
		print "Loading data for container", index
		container = Container(self.backup,index)

		filename = container.filename()+".data"
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		return target_path
	def save_container(self,container):
		index = container.index
		
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		
		if staging_path != target_path:
			shutil.move(staging_path, target_path)
			shutil.move(staging_path+".data", target_path+".data")

import smtplib
from email import Encoders
from email.Message import Message
from email.MIMEAudio import MIMEAudio
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEImage import MIMEImage
from email.MIMEText import MIMEText


class MailContainerConfig(ContainerConfig):
	"""
	Handler for gmail container.
	Needs a list of gmail addresses.
	"""
	def __init__(self):
		ContainerConfig.__init__(self)
		self.accounts = []
	def init(self,backup,txn_handler,params):
		ContainerConfig.init(self,backup,txn_handler)
		self.backup = backup
		self.username = params[0]
		self.password = params[1]
		self.quota = int(params[2])
	def configure(self,username,password,quota):
		self.add_account(username,password,quota)
	def load(self,filename):
		file = open(filename, "rb")
		ContainerConfig.load(self,file)
		configLine = file.readline()
		(username,password,quota) = re.split("\s+",configLine)[0:3]
		self.add_account(username,password,quota)
	def save(self,filename):
		file = open(filename,"wb")
		ContainerConfig.save(self,file)
		account = self.accounts[0]
		file.write("%s %s %s" % (account["user"],account["pass"],account["quota"]))
	def add_account(self,username,password,quota):
		self.accounts.append({"user":username, "pass":password, "quota":quota, "used":0})
	def container_size(self):
		return 2<<20
	def save_container(self,container):
		print "Saving container"

		s = smtplib.SMTP()
		s.set_debuglevel(1)
		print "connecting"
		#s.connect("gmail-smtp-in.l.google.com")
		s.connect("alt2.gmail-smtp-in.l.google.com")
		#s.connect("smtp.gmail.com", 587)
		print "starting tls"
		s.ehlo("www.manent.net")
		s.starttls()
		s.ehlo("www.manent.net")
		print "logging in"
		
		print "sending header in mail"
		#s.set_debuglevel(0)
		header_msg = MIMEMultipart()
		header_msg["Subject"] = "manent.%s.%s.header" % (self.backup.label, str(container.index))
		#header_msg["To"] = "gsasha@gmail.com"
		#header_msg["From"] = "gsasha@gmail.com"
		header_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		header_file = open(os.path.join(self.backup.global_config.staging_area(),filename), "rb")
		header_attch.set_payload(header_file.read())
		header_file.close()
		Encoders.encode_base64(header_attch)
		header_msg.attach(header_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com", "gsasha.manent1@gmail.com", header_msg.as_string())
		s.set_debuglevel(1)
		
		print "sending data in mail"
		data_msg = MIMEMultipart()
		data_msg["Subject"] = "manent.%s.%s.data" % (self.backup.label, str(container.index))
		data_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		data_file = open(os.path.join(self.backup.global_config.staging_area(),filename+".data"), "rb")
		data_attch.set_payload(data_file.read())
		data_file.close()
		Encoders.encode_base64(data_attch)
		data_msg.attach(data_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com", "gsasha.manent1@gmail.com", data_msg.as_string())
		s.set_debuglevel(1)
		print "all done"
		
		s.close()
		print header_msg.as_string()

class OpticalContainerConfig(ContainerConfig):
	"""
	Handler for optical container.
	Can be one of: CD-650, CD-700, DVD, DVD-DL, BLURAY :)
	"""
	CONTAINER_TYPES = {
		"CD-650" : 650<<20,
		"CD-700" : 700<<20,
		"DVD"    : 4600<<20,
		"DVD-DL" : 8500<<20,
		"BLURAY" : 26000<<20,
		}
	def __init__(self):
		self.containerType = NONE
		self.containers = []
		raise "not implemented"
	def configure(self,params):
		(containerType,) = params
		self.containerType = containerType
		if not CONTAINER_TYPES.has_key(self.containerType):
			print "Unknown container type", self.containerType
			exit(1)
	def load(self,filename):
		file = open(filename, "rb")
		ContainerConfig.load(self,file)
		containerType = file.readline()
		for line in file:
			self.containers.append(line)
	def save(self,file):
		file = open(filename,"wb")
		ContainerConfig.save(self,file)
		file.write(self.containerType+"\n")
		for container in self.containers:
			file.write(container+"\n")
	def container_size(self):
		return CONTAINER_TYPES[self.containerType]

