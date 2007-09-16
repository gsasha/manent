from cStringIO import StringIO
import os, os.path, shutil
import re
import base64
import bz2,zlib
import traceback
import sys
import Crypto.Cipher.ARC4

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
#      - size field is the size of the encrypted data (padding might have been used
#        for some encryption algorithms
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

CODE_CONTROL_START         = 48
#
# Codes for both kinds of blocks
#
CODE_COMPRESSION_END       = 48
CODE_COMPRESSION_BZ2       = 49
CODE_COMPRESSION_GZIP      = 50

CODE_ENCRYPTION_END        = 64
CODE_ENCRYPTION_ARC4       = 65

def compute_packer_code(code):
	assert code < CODE_COMPRESSION_END
	if code % 2 == 0:
		return code+1
	else:
		return code

def is_packer_code(code):
	assert code < CODE_COMPRESSION_END
	return code%2==1

#-------------------------------------------------------------------
# Dump creation and reading utilities
#-------------------------------------------------------------------
class DataDumper:
	def __init__(self,file):
		self.file = file
		self.blocks = []

		self.total_size = 0
		
		self.encryptor = None
		self.compressor = None

	def add_block(self,digest,data,code):
		self.blocks.append((digest,len(data),code))
		print "Adding block %d:%s[%s]" % (code,base64.b64encode(digest),base64.b16encode(data))
		print "File position", self.total_size, self.file.tell()
		print "Plain data: ", base64.b16encode(data)
		if self.compressor is not None:
			data = self.__compress(data)
			print "Compressed data: ", base64.b16encode(data)
		if self.encryptor is not None:
			data = self.__encrypt(data)
			print "Encrypted data : ", base64.b16encode(data)
		self.file.write(data)
		self.total_size += len(data)

	#
	# Encryption support
	#
	def start_encryption(self,algorithm_code,seed,password):
		"""
		Encryption can be started only when compression is inactive
		"""
		assert self.encryptor is None
		assert self.compressor is None
		
		self.blocks.append((seed,0,algorithm_code))
		print "%s CODE_ENCRYPTION_ARC4 start" % (base64.b64encode(seed))
		print "file position", self.total_size, self.file.tell()
		if algorithm_code == CODE_ENCRYPTION_ARC4:
			key = Digest.dataDigest(seed+password)
			self.encryptor = Crypto.Cipher.ARC4.new(key)
		self.encrypted_data_size = 0
		self.encrypted_data_digest = Digest.DataDigestAccumulator()
	def stop_encryption(self):
		assert self.encryptor is not None
		assert self.compressor is None

		self.blocks.append((self.encrypted_data_digest.digest(),
		                    self.encrypted_data_size,CODE_ENCRYPTION_END))
		self.encryptor = None
	def __encrypt(self,data):
		self.encrypted_data_digest.update(data)
		print "Encryption position", self.encrypted_data_size
		self.encrypted_data_size += len(data)
		return self.encryptor.encrypt(data)
	#
	# Compression support
	#
	def start_compression(self,algorithm_code):
		"""
		Compression can be started under encryption
		"""
		assert self.compressor is None

		digest = Digest.dataDigest(str(len(self.blocks)))
		print "Starting compression %s" % (base64.b64encode(digest))
		print "file position", self.total_size, self.file.tell()
		self.blocks.append((digest,0,algorithm_code))
		if algorithm_code == CODE_COMPRESSION_BZ2:
			self.compressor = bz2.BZ2Compressor(9)
		elif algorithm_code == CODE_COMPRESSION_GZIP:
			self.compressor = zlib.compressobj()
		else:
			raise Exception("Unsupported compression algorithm")
		self.compressor_algorithm = algorithm_code
		self.uncompressed_size = 0
		self.compressed_size = 0

	def stop_compression(self):
		assert self.compressor is not None
		tail = self.compressor.flush()
		self.compressed_size += len(tail)
		
		print "Compressed data: ", base64.b16encode(tail)
		print "file position", self.total_size, self.file.tell()
		if self.encryptor is not None:
			tail = self.__encrypt(tail)
			print "Encrypted data: ", base64.b16encode(tail)
		self.file.write(tail)
		self.total_size += len(tail)
		print "Wrote data to file"
		print "file position", self.total_size, self.file.tell()
		self.blocks.append((Digest.dataDigest(""),self.compressed_size,CODE_COMPRESSION_END))
		self.compressor = None

	def __compress(self,data):
		self.uncompressed_size += len(data)
		compressed = self.compressor.compress(data)
		self.compressed_size += len(compressed)
		# The following should be necessary according to the documentation on zlib module
		# However, I don't see that the compressor has unconsumed_tail attribute!
		#if self.compressor_algorithm == CODE_COMPRESSION_GZIP:
			#while self.compressor.unconsumed_tail != "":
				#print "Feeding unconsumed tail of length %d to the compressor" % len(self.compressor.unconsumed_tail)
				#compressed += self.compressor.compress(self.compressor.unconsumed_tail)
		return compressed
	#
	# Result
	#
	def get_blocks(self):
		return self.blocks

class DataDumpLoader:
	"""The only mode of loading blocks from a container is through a handler.
	The handler can determine, given a digest and a code, whether a given block
	should be loaded. If the block is loaded, the handler returns it back to the
	handler through callback."""
	def __init__(self,file,blocks,password):
		self.file = file
		self.blocks = blocks
		self.password = password

		self.uncompressor = None
		self.decryptor = None

	def load_blocks(self,handler):
		total_offset = 0
		uncompressed_offset = 0
		skip_until = None
		for i in range(len(self.blocks)):
			(digest,size,code) = self.blocks[i]
			if code == CODE_ENCRYPTION_ARC4:
				# Since encryption blocks are not nested in anything,
				# we can't see start of encryption when skipping
				assert skip_until is None
				print "file position", self.file.tell()
				# find out if any of the blocks contained within
				# the section is actually needed
				requested = False
				for j in range(i+1,len(self.blocks)):
					(s_digest,s_size,s_code) = self.blocks[j]
					if s_code == CODE_ENCRYPTION_END:
						break
					if handler.is_requested(s_digest,s_code):
						print "Requested block %d:%s" % (s_code, base64.b64encode(s_digest))
						requested = True
				else:
					raise Exception("Block table error: encryption start without end")

				print "%s CODE_ENCRYPTION_ARC4 goes for %d rounds" % (base64.b64encode(digest), j-i),
				if requested:
					print "has requested blocks"
				else:
					print "has no requested blocks"

				if not requested:
					skip_until = CODE_ENCRYPTION_END

				# We always perform decryption, even when it's needed only for checking
				key = Digest.dataDigest(digest+self.password)
				self.decryptor = Crypto.Cipher.ARC4.new(key)
				self.decryptor_data_digest = Digest.DataDigestAccumulator()
				self.decrypted_bytes = 0
				
			elif code == CODE_ENCRYPTION_END:
				# Encryption cannot be nested in compression
				assert skip_until != CODE_COMPRESSION_END
				print "CODE_ENCRYPTION_END"
				if skip_until == CODE_ENCRYPTION_END:
					print "reading %d from position %d" % (size,self.file.tell())
					skipped = self.file.read(size)
					print "Decrypting skipped data", base64.b16encode(skipped)
					skipped = self.decryptor.decrypt(skipped)
					self.decrypted_bytes += len(skipped)
					print "Decrypted skipped data", base64.b16encode(skipped)
					self.decryptor_data_digest.update(skipped)
					skip_until = None
				assert self.decryptor_data_digest.digest() == digest
				self.decryptor = None
				self.decryptor_data_digest = None
			
			#
			# Process compression tags
			#
			elif code == CODE_COMPRESSION_BZ2:
				if skip_until is not None:
					assert skip_until == CODE_ENCRYPTION_END
					print "%s CODE_COMPRESSION_BZ2 skipping" % base64.b64encode(digest), self.file.tell()
					continue
				# find out if any of the blocks contained within
				# the section is actually needed
				requested = False
				for j in range(i+1,len(self.blocks)):
					s_digest,s_size,s_code = self.blocks[j]
					if s_code == CODE_COMPRESSION_END:
						self.uncompress_bytes = s_size
						break
					if handler.is_requested(s_digest,s_code):
						requested = True
				else:
					raise Exception("Block table error: compression start without end")

				print "%s CODE_COMPRESSION_BZ2 goes for %d rounds" % (base64.b64encode(digest), j-i),
				if requested:
					print "has requested blocks"
				else:
					print "has no requested blocks"

				if requested:
					self.uncompressor = bz2.BZ2Decompressor()
					self.uncompressed_buf = ""
				else:
					skip_until = CODE_COMPRESSION_END
			elif code == CODE_COMPRESSION_GZIP:
				if skip_until is not None:
					assert skip_until == CODE_ENCRYPTION_END
					print "%s CODE_COMPRESSION_GZIP skipping" % base64.b64encode(digest),self.file.tell()
					continue
				# find out if any of the blocks contained within
				# the section is actually needed
				requested = False
				for j in range(i+1,len(self.blocks)):
					s_digest,s_size,s_code = self.blocks[j]
					if s_code == CODE_COMPRESSION_END:
						self.uncompress_bytes = s_size
						break
					if handler.is_requested(s_digest,s_code):
						requested = True
				else:
					raise Exception("Block table error: compression start without end")

				print "%s CODE_COMPRESSION_GZIP goes for %d rounds" % (base64.b64encode(digest), j-i),
				if requested:
					print "has requested blocks"
				else:
					print "has no requested blocks"

				if requested:
					self.uncompressor = zlib.decompressobj()
					self.uncompressed_buf = ""
				else:
					skip_until = CODE_COMPRESSION_END
			
			elif code == CODE_COMPRESSION_END:
				if skip_until == CODE_ENCRYPTION_END:
					assert self.uncompressor is None
					print "%s CODE_COMPRESSION_END skipping" % base64.b64encode(digest)
					continue
				print "CODE_COMPRESSION_END"
				if skip_until == CODE_COMPRESSION_END:
					data = self.file.read(size)
					if self.decryptor is not None:
						data = self.decryptor.decrypt(data)
						self.decryptor_data_digest.update(data)
					skip_until = None
				else:
					if self.uncompress_bytes != 0:
						chunk = self.file.read(self.uncompress_bytes)
						print "reading %d unused compressor bytes" % self.uncompress_bytes
						print "file position", self.file.tell()
						if self.decryptor is not None:
							print "Decrypting unused bytes"
							chunk = self.decryptor.decrypt(chunk)
							print "Decryption position", self.decrypted_bytes
							self.decrypted_bytes += len(chunk)
							self.decryptor_data_digest.update(chunk)
						else:
							print "No decryptor found"
				self.uncompressor = None
				self.uncompressed_buf = ""
			#
			# Read normal data
			#
			else:
				if skip_until is not None:
					print "Skipping until", skip_until
					continue
				#print "normaldata:",self.decryptor
				# If we're not skipping, we must also read, to preserve
				# consistency of the blocks
				print "Data block"
				print "file position", self.file.tell()
				# Uncompress data if necessary
				if self.uncompressor is not None:
					data = ""
					while len(data) < size:
						if len(self.uncompressed_buf) > 0:
							portion = min(size-len(data),len(self.uncompressed_buf))
							data += self.uncompressed_buf[:portion]
							self.uncompressed_buf = self.uncompressed_buf[portion:]
						else:
							toread = min(8192, self.uncompress_bytes)
							self.uncompress_bytes -= toread
							chunk = self.file.read(toread)
							if self.decryptor is not None:
								print "decrypting chunk", base64.b16encode(chunk)
								chunk = self.decryptor.decrypt(chunk)
								self.decryptor_data_digest.update(chunk)
								print "Decryption position", self.decrypted_bytes
								self.decrypted_bytes += len(data)
							if len(chunk) < toread:
								raise Exception("Cannot read data expected in the container")
							print "uncompressing chunk", base64.b16encode(chunk)
							self.uncompressed_buf = self.uncompressor.decompress(chunk)
				else:
					data = self.file.read(size)
					print "reading chunk", base64.b16encode(data)
					if self.decryptor is not None:
						data = self.decryptor.decrypt(data)
						self.decryptor_data_digest.update(data)
						print "Decryption position", self.decrypted_bytes
						self.decrypted_bytes += len(data)
						print "decrypted chunk", base64.b16encode(data)

				if handler.is_requested(digest,code):
					print "block %s:%d is requested" %(base64.b64encode(digest),code)
					handler.loaded(digest,data,code)
				else:
					print "block %s:%d is not requested" %(base64.b64encode(digest),code)
					pass
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
	def __init__(self,storage,header_file_name,body_file_name):
		# Configuration data
		self.storage = storage
		self.header_file_name = header_file_name
		self.body_file_name = body_file_name
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
		self.blocks.append((Digest.dataDigest(""),self.totalSize,CODE_COMPRESSION_BZ2))
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
			elif code == CODE_COMPRESSION_BZ2:
				print " BZ2 START AT %d" % (size)
			elif code == CODE_COMPRESSION_GZIP:
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
			if code == CODE_COMPRESSION_BZ2:
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
			elif code == CODE_COMPRESSION_BZ2:
				compression_start_offset = size
				block_offset = 0
				last_read_offset = 0
				file.seek(size)
				#print "Starting bzip2 decompressor"
				decompressor = BZ2FileDecompressor(file,compression_sizes[size])
			elif code == CODE_COMPRESSION_GZIP:
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
				# All the blocks except CODE_COMPRESSION_BZ2 use size for really size
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

