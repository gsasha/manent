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

class NoopCompressor:
	def compress(self,data):
		return data
	def flush(self):
		return ""

#---------------------------------------------------
# Container file format:
#
# Each container consists of two files, the header and the body.
# The header contains its own header and blocks that encode metadata
# One of the blocks is the BLOCK_TABLE that describes the blocks stored
# in the body
#
# Header file format:
# 1. magic number "MNNT"
# 2. version number
# 3. digest of the header's header
# 4. header table length
# 5. header table entries in format:
#    5.1 entry size
#    5.2 entry code
#    5.3 entry digest
# 6. entries, encoded according to the header table
#
# Body file format:
# 1. entries, encoding according to the body table
#
# Table encoding
# Table contains three types of tags:
# 1. Compression control
#    COMPRESSION_START_<ALGORITHM>
#     The following entries are compressed by the given algorithm.
#      - size field gives the offset from which to read the compressed data
#      - digest is unused
#    COMPRESSION_END
#     The following entries are no longer compressed.
#      - size gives the total size of the *compressed* data
#      - digest is unused
#    Compression blocks should not be nested
# 2. Encryption control
#    We assume that the encryption does not change the size of the encrypted data.
#    ENCRYPTION_START_<ALGORITHM>
#     The following entries are encrypted by the given algorithm.
#      - size field is the starting offset of the data
#      - digest field gives the seed used for the encryption.
#        Data for the digest should be taken from a random source.
#        For example, for arc4 encryption:
#        key = Digest(seed+password)
#        encryptor = ARC4(key)
#     The start tag should always be closed with an END tag.
#    ENCRYPTION_END
#     The following entries are no longer encrypted.
#      - size field is the size of the encrypted data
#      - digest field gives the hash of the plain data, used to verify that
#        the decryption was successful.
#    Compression blocks can be nested within encryption blocks.
#    The reverse is not a good idea (encrypted data would not compress).
# 3. Data blocks
#    Data blocks can have different codes, which are irrelevant here but have
#    a meaning for the application.
#     - size field is the starting offset of the data.
#       If the block is nested in a Compression block, then the offset is within
#       the uncompressed data (but of course! There is no meaning to offset within
#       the compressed data).
#     - digest field gives the digest of the data, for use by the application
#       and for verification
#---------------------------------------------------

#
# Codes for blocks stored in the body
#
CODE_DATA                  =  0
CODE_DATA_PACKER           =  1
CODE_DIR                   =  2
CODE_DIR_PACKER            =  3

#
# Codes for blocks stored in the header
#
CODE_CONTAINER_DESCRIPTOR  = 16
CODE_BLOCK_TABLE           = 17
CODE_INCREMENT_START       = 18
CODE_INCREMENT_INTERMEDIATE= 19
CODE_INCREMENT_END         = 20

#
# Codes for both kinds of blocks
#
CODE_COMPRESSION_END       = 48
CODE_COMPRESSION_BZ2_START = 49
CODE_COMPRESSION_GZIP_START= 50

CODE_ENCRYPTION_END        = 64
CODE_ENCRYPTION_ARC4_PWD   = 65

def compute_packer_code(code):
	assert code < CODE_COMPRESSION_END
	if code % 2 == 0:
		return code+1
	else:
		return code

def is_packer_code(code):
	assert code < CODE_COMPRESSION_END
	return code%2==1

class Container:
	"""
	Represents one contiguous container that can be saved somewhere, i.e.,
	on an optical disk, in a mail system, over the network etc.

	Container consists in one of two states:
	1. Normal - in this state, blocks are added to the container
	2. Frozen - in this state, the container is completed. It can be written
	            out, or its blocks can be read back.

	Container can have its contents encrypted - TODO
	Container can have its contents compressed

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
		self.blocks = None

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
		numBlocks = Format.read_int(tableFile)
		for i in range(0,numBlocks):
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
		self.frozen = True
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

