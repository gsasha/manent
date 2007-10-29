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
		self.config = ConfigParser.ConfigParser()
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
		for backup in self.open_backups:
			# Save the data for the backup
			pass
	def close(self):
		#for backup in self.open_backups:
		#	backup.close()
		pass
	
	def create_backup(self,label,root):
		if self.backups_config.has_section("backups/"+label):
			raise "Backup %s already exists"%label

		print "Creating backup label[%s] path[%s]"%(label, root)
		backup = Backup.Backup(self,label)
		backup.configure(root)
		self.open_backups.append(backup)
		
		return backup
	def load_backup(self,label):
		backup_prefix = "backups/"+label
		if not self.config.has_section(backup_prefix):
			raise "Backup %s does not exist"%label
		
		backup = Backup.Backup(self,label)
		self.open_backups.append(backup)
		
		data_root = self.config.get(backup_prefix,"data_root")
		for section in self.config.sections():
			if section.startswith(backup_prefix+"/excludes"):
				self.load_backup_excludes(backup,section)
			if section.startswith(backup_prefix+"/storages/"):
				self.load_backup_storages(backup,section)
		return backup
	def save_backup(self,backup):
		# Remove and re-create the section to make sure it's empty
		for section in self.config.sections():
			if section.startswith("backups/"+backup.get_label):
				section.remove
		self.config.remove_section(backup.get_label())
		self.config.add_section(backup.get_label())
	def load_backup_excludes(self,backup,section):
		for key,value in sorted(self.config.items(section)):
			if key.endswith("exclude_regexp"):
				backup.add_exclude_regexp(value)
			elif key.endswith("include_regexp"):
				backup.add_include_regexp(value)
			elif key.endswith("exclude"):
				backup.add_exclude(value)
			elif key.endswith("include"):
				backup.add_include(value)
	def save_backup
	def remove_backup(self,label):
		if not self.backups.has_key(label):
			raise "Backup %s does not exist"%label
		backup = Backup.Backup(self,label)
		backup.remove()
		del self.backups[label]
		
	def has_backup(self,label):
		return self.backups.has_key("backups/"+label)
	def list_backups(self):
		for key in self.backups_config.sections():
			match = re.match("backups/([^/]+)",key)
			if match:
				print "Backup", match.group(1)
		return self.backups.keys()
	def get_backup(self, label):
		return self.backups[label]


