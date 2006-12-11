import md5
import struct
import base64

import Format

class VersionConfig:
	def __init__(self):
		pass

	def version(self):
		return "0.1"
	#
	# Hashing parameters
	#
	def dataDigest(self,data):
		#return hashlib.sha256(data).digest()
		h = md5.new(struct.pack("B",len(data)%256))
		h.update(data)
		return h.digest()
	def dataDigestSize(self):
		#return 32
		return 16
	def headerDigest(self,data):
		#return hashlib.md5(data).digest()
		return md5.md5(data).digest()
	def headerDigestSize(self):
		return 16
	#
	# Value reading/writing
	#
	def write_int(self,file,num):
		file.write(Format.binary_encode_int_varlen(num))
	def read_int(self,file):
		return Format.binary_read_int_varlen(file)
	def read_ints(self,file):
		return Format.binary_read_int_varlen_list(file)
	def write_ints(self,file,nums):
		for num in nums:
			file.write(Format.binary_encode_int_varlen(num))
	def write_string(self,file,str):
		"""
		Write a Pascal-encoded string of length of up to 2^16
		"""
		file.write(Format.binary_encode_int_varlen(len(str)))
		file.write(str)
	def read_string(self,file):
		"""
		The reverse of write_string
		"""
		length = binary_read_int_varlen(file)
		return file.read(length)
	#
	# Block parameters
	#
	def blockSize(self):
		return 256*1024
	def compression_block_size(self):
		return 2*1024*1024

	#
	# Node configuration
	#
	def node_key(self,num):
		return binary_encode_int_varlen(num)
		#return "N%x"%num

