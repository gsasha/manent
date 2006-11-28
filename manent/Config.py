#import hashlib
import md5
import struct
import os, os.path, sys
import re
import base64

import Backup
import Container

def ascii_encode_int_varlen(num):
	"""
	Variable-length coding of numbers that will appear in correct order when
	sorted lexicographically.
	"""
	s = "%x"%num
	return chr(ord('a')-1+len(s))+s

def ascii_decode_int_varlen(s):
	"""
	Decoding for the above format.
	"""
	print "decoding",s
	if ord(s[0])-ord('a') != len(s)-2:
		raise "malformed ascii int encoding: %d != %d" %(ord(s[0])-ord('a'),len(s)-2)
	return int(s[1:],16)

def ascii_read_int_varlen(file):
	"""
	Reading of the above coded number from a file
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# end of file
		return None
	bytes = file.read(ord(first_byte)-ord('a')+1)
	return ascii_decode_int_varlen(first_byte+bytes)

def binary_encode_int_varlen(num):
	"""
	Variable-length coding of nonnegative numbers that includes the length symbol, so that
	it is stored quite compactly. The first byte is the number of bytes in the code,
	so a list of such numbers can be read serially from a file.

	The encoding goes as follows:

	For numbers 0<=x<=127, the encoding is exactly one byte, the number itself (MSBit is 0)
	For larger numbers, the first byte specifies the length (the MSBit or the byte is 1 to denote this case).
	The rest of the bytes contain the encoding of the number, from most significant to less significant.

	This encoding still preserves lexicographical order!
	"""
	if num < 0:
		raise "Negative numbers are not supported"
	if num < 128:
		return chr(num)
	bytes = []
	while num != 0:
		bytes.append(chr(num%256))
		num /= 256
	bytes.reverse()
	return chr(len(bytes)+128-1)+"".join(bytes)

def binary_decode_int_varlen(s):
	"""
	Decoding for the above format
	"""
	l = ord(s[0])
	if l < 128:
		return l
	l -= 128
	if l != (len(s)-2):
		raise "malformed binary int encoding"
	res = 0
	for byte in s[1:]:
		res *= 256
		res += ord(byte)
	return res

def binary_read_int_varlen(file):
	"""
	Read one integer in the above encoding
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# End of file
		return None
	if ord(first_byte) < 128:
		return ord(first_byte)
	bytes = file.read(ord(first_byte)-128+1)
	return binary_decode_int_varlen(first_byte+bytes)

class GlobalConfig:
	def __init__(self):
		self.backups = {}
		self.open_backups = []

		self.staging_area_exists = False
		self.home_area_exists = False

	def version(self):
		return "0.1"
	#
	# Filesystem config
	#

	def block_file_name(self,digest):
		return self.stagingArea()+"block."+base64.urlsafe_b64encode(digest)
	def container_file_name(self,label,index):
		return "manent.%s.%s" % (label,ascii_encode_int_varlen(index))
	def container_index(self,name,label,suffix):
		file_regexp = "^manent\\.%s\\.(\d+)%s$"%(label,suffix)
		match = re.match(file_regexp, name)
		if not match:
			return None
		index = ascii_decode_int_varlen(match.group(1))
		return index
	
	def home_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["APPDATA"], "manent")
		else:
			path = os.path.join(os.environ["HOME"], "manent")
		if not self.home_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.home_area_exists = True
		return path
		
	def staging_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["TEMP"], "manent.staging")
		else:
			path = "/tmp/manent.staging"
		if not self.staging_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.staging_area_exists = True
		return path
	
	def load(self):
		if not os.path.exists(self.home_area()+"/config"):
			return
		file = open(self.home_area()+"/config")
		for line in file:
			(label,dataPath,containerType, containerParams) = re.split("\s+",line+" ",3)
			containerParams = re.split("\s+", containerParams.rstrip())
			self.backups[label] = (dataPath, containerType, containerParams)
	def save(self):
		file = open(self.home_area()+"/config","w")
		for label in self.backups.keys():
			(dataPath,containerType,containerParams) = self.backups[label]
			file.write("%s %s %s %s\n" % (label, dataPath, containerType, " ".join(containerParams)))
	def close(self):
		for backup in self.open_backups:
			backup.close()
	
	def create_backup(self,label,dataPath,containerType,containerParams):
		if self.backups.has_key(label):
			raise "Backup %s already exists"%label

		print "Creating backup label[%s] path[%s] type[%s] params[%s]"%(label, dataPath, containerType, str(containerParams))
		backup = Backup.Backup(self,label)
		backup.configure(dataPath,containerType,containerParams)
		self.open_backups.append(backup)
		
		self.backups[label] = (dataPath,containerType,containerParams)
		return backup
	def load_backup(self,label):
		if not self.backups.has_key(label):
			raise "Backup %s does not exist"%label
		
		backup = Backup.Backup(self,label)
		(dataPath,containerType,containerParams) = self.backups[label]
		print self.backups[label]
		backup.load(dataPath,containerType,containerParams)
		self.open_backups.append(backup)
		return backup
	def reconstruct_backup(self,label,dataPath,containerType,containerParams):
		if self.backups.has_key(label):
			raise "Backup %s already exists" % label
		
		backup = Backup.Backup(self,label)
		backup.reconstruct(dataPath,containerType,containerParams)
		
		self.open_backups.append(backup)
		self.backups[label] = (dataPath,containerType,containerParams)
		return backup
	def remove_backup(self,label):
		if not self.backups.has_key(label):
			raise "Backup %s does not exist"%label
		backup = Backup.Backup(self,label)
		backup.remove()
		del self.backups[label]
		
	def has_backup(self,label):
		return self.backups.has_key(label)
	def list_backups(self):
		return self.backups.keys()
	def get_backup(self, label):
		return self.backups[label]


# TODO: Make ContainerConfig a subclass of Config ???

class Config:
	def __init__(self):
		pass

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
		file.write(binary_encode_int_varlen(num))
	def read_int(self,file):
		return binary_read_int_varlen(file)
	def write_string(self,file,str):
		"""
		Write a Pascal-encoded string of length of up to 2^16
		"""
		file.write(binary_encode_int_varlen(len(str)))
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

