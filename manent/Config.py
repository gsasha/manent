#import hashlib
import md5
import struct
import os, os.path, sys
import re
import base64

import Backup
import Container

class GlobalConfig:
	def __init__(self):
		self.backups = {}
		self.open_backups = []

		self.staging_area_exists = False
		self.home_area_exists = False
		
	#
	# Filesystem config
	#

	def block_file_name(self,digest):
		return self.stagingArea()+"block."+base64.urlsafe_b64encode(digest)
	def container_file_name(self,label,index):
		return "manent.%s.%d" % (label,index)
	def container_index(self,name,label,suffix):
		file_regexp = "^manent\\.%s\\.(\d+)%s$"%(label,suffix)
		match = re.match(file_regexp, name)
		if not match:
			return None
		index = int(match.group(1))
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
		
	def has_backup(self,label):
		return self.backups.has_key(label)
	def list_backups(self):
		return self.backups.keys()
	def get_backup(self, label):
		return self.backups[label]


# TODO: Make ContainerConfig a subclass of Config

class Config:
	def __init__(self):
		pass

	#
	# Hashing parameters
	#
	def dataDigest(self,data):
		#return hashlib.sha256(data).digest()
		data = ("%8x"%len(data))+data
		return md5.md5(data).digest()
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
		file.write(struct.pack("!Q",num))
	def read_int(self,file):
		string = file.read(8)
		if string == '':
			return None
		(result,) = struct.unpack("!Q",string)
		return result
	def write_string(self,file,str):
		"""
		Write a Pascal-encoded string of length of up to 2^16
		"""
		file.write(struct.pack("!H",len(str)))
		file.write(str)
	def read_string(self,file):
		"""
		The reverse of write_string
		"""
		string = file.read(2)
		if string=='':
			return None
		(len,) = struct.unpack("!H",string)
		return file.read(len)
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
		return "N%x"%num

	