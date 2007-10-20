#import hashlib
import os, os.path, sys
import re, fnmatch
import ConfigParser

import Backup
import Container
import manent.utils.Format as Format
import manent.utils.IntegerEncodings as IntegerEncodings

class GlobalConfig:
	def __init__(self):
		self.backups_config = ConfigParser.ConfigParser()
		self.options_config = ConfigParser.ConfigParser()
		self.open_backups = []

		self.staging_area_exists = False
		self.home_area_exists = False

		self.excludes_list = None

	#
	# Filesystem config
	#
	def container_file_name(self,label,index):
		return "manent.%s.%s" % (label,IntegerEncodings.ascii_encode_int_varlen(index))
	def container_index(self,name,label,suffix):
		file_regexp = "^manent\\.%s\\.(\w\w+)%s$"%(label,suffix)
		match = re.match(file_regexp, name)
		if not match:
			return None
		index = IntegerEncodings.ascii_decode_int_varlen(match.group(1))
		return index
	
	def home_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["APPDATA"], "manent")
		else:
			path = os.path.join(os.environ["HOME"], ".manent")
		if not self.home_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.home_area_exists = True
		return path

	def excludes(self):
		"""
		Hardcode several paths that we really don't want to backup.
		Must do more extensive work later, using an explicit filter
		"""
		if self.excludes_list != None:
			return self.excludes_list

		def prefix_check_maker(prefix):
			"""
			Generator function that builds a single prefix checker
			"""
			def prefix_checker(s):
				return s.startswith(prefix)
			return prefix_checker
		
		def regexp_check_maker(pattern):
			"""
			Generator function that builds a single path checker
			"""
			regexp = re.compile(pattern)
			def match_checker(s):
				return regexp.match(s)
			return match_checker
		
		if os.name == "nt":
			base = os.environ["APPDATA"]
		else:
			base = os.environ["HOME"]

		cp = ConfigParser.ConfigParser()
		cp.read([os.path.join(self.home_area(),"excludes")])

		def is_relative(pat):
			if os.name == "nt":
				return re.match("\w:",pat)
			else:
				return pat.startswith("/")

		#
		# Home are is always excluded from backups!
		# TODO: replace prefixes and regexps by fnmatch
		#
		self.excludes_list = [prefix_check_maker(self.home_area())]
		if cp.has_section("EXCLUDE/REGEXP"):
			for key,val in cp.items("EXCLUDE/REGEXP"):
				if not is_relative(val):
					val = os.path.join(base,val)
				print "excluding regexp",val
				self.excludes_list += [regexp_check_maker(val)]
		if cp.has_section("EXCLUDE/PREFIX"):
			for key,val in cp.items("EXCLUDE/PREFIX"):
				if not is_relative(val):
					val = os.path.join(base,val)
				print "excluding prefix",val
				self.excludes_list += [prefix_check_maker(val)]

		return self.excludes_list

	def staging_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["TEMP"], "manent.staging")
		else:
			path = "/tmp/manent.staging."+os.environ["USER"]
		if not self.staging_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.staging_area_exists = True
		return path
	
	#
	# Configuration persistence
	#
	def load(self):
		self.backups_config.read(self.home_area()+"/backups.ini"))
	def save(self):
		self.config_parser.write(open(self.home_area()+"/backups.ini","w"))
		for label in self.backups.keys():
			(dataPath,containerType,containerParams) = self.backups[label]
			file.write("%s %s %s %s\n" % (label, dataPath, containerType, " ".join(containerParams)))
	def close(self):
		#for backup in self.open_backups:
		#	backup.close()
		pass
	
	def create_backup(self,label,root):
		if self.backups_config.has_section(label):
			raise "Backup %s already exists"%label

		print "Creating backup label[%s] path[%s]"%(label, root)
		backup = Backup.Backup(self,label)
		backup.configure(root)
		self.open_backups.append(backup)
		
		self.backups[label] = (dataPath,containerType,containerParams)
		return backup
	def load_backup(self,label):
		if not self.backups_config.has_section(label):
			raise "Backup %s does not exist"%label
		
		backup = Backup.Backup(self,label)
		data_root = self.backups_config.get(label,"data_root")
		for item,value in self.backups_config.items(label):
			if item.startswith("storage"):
		(dataPath,containerType,containerParams) = self.backups[label]
		#print self.backups[label]
		backup.configure(dataPath,containerType,containerParams)
		self.open_backups.append(backup)
		return backup
	def reconstruct_backup(self,label,dataPath,containerType,containerParams):
		# TODO: there is no more such thing as reconstruct!
		# Just adding a storage to the list of backups will cause that storage
		# to be rescanned, which is the same as reconstruction
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


