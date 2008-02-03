#import hashlib
import os, os.path, sys
import re, fnmatch
import ConfigParser

import Backup
import Container
import manent.utils.IntegerEncodings as IntegerEncodings

# Need to decide how do I store all the configuration:
# - Do I store all in the config file ~/.manent/config
#   Pros: The configuration is stored in a centralized location,
#         and one class can be dedicated to manage it
#   Pros: The configuration is user-readable and user-writable.
#         If it needs user intervention, it will be easier
#         (from "the inmates": there should be no need for user intervention!)
# - Do I store only the basic data in the config file, and the rest
#   in the database?
#   Pros: The data is stored where it is usualy consumed
#   Cons: Sometimes the data must be accessed from the outside. The classes
#         that store the data must provide some query interface
#   Cons: There will be an artificial split, since some data needs to be
#         still stored in the configuration file.
#   Pros: On the other hand, some information still needs to be stored
#         privately
#
# Ok, let's write down which data we need:
# 1. The list of backups
#    For each backup
# 1.1 Exclusion/Inclusion patterns
# 1.2 Repository configuration
#     List of storages. For each storage:
# 1.2.1 Location specification
# 1.2.2 Private information required by the Repository class
# 2. Global configuration parameters:
# 2.1. Global exclusion/inclusion patterns
#
# The information will be stored in the following databases:
# ~/.manent/config.db:global: the global configuration
#   backups=<list of backups>
#
# The lists of names are stored as arrays of strings.
class Paths:
	def __init__(self):
		self.staging_area_exists = False
		self.home_area_exists = False
	def home_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["APPDATA"], "manent1")
		else:
			path = os.path.join(os.environ["HOME"], ".manent1")
		if not self.home_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.home_area_exists = True
		return path

	def staging_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["TEMP"], "manent.staging")
		else:
			path = "/tmp/manent.staging."+os.environ["USER"]
		if not self.staging_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.staging_area_exists = True
		return path

paths = Paths()

class GlobalConfig:
	def __init__(self):
		self.config_parser = ConfigParser.ConfigParser()
		self.open_backups = []

		self.staging_area_exists = False
		self.home_area_exists = False

	def home_area(self):
		if os.name == "nt":
			path = os.path.join(os.environ["APPDATA"], "manent")
		else:
			path = os.path.join(os.environ["HOME"], ".manent")
		if not self.home_area_exists and not os.path.exists(path):
			os.mkdir(path)
			self.home_area_exists = True
		return path

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
		self.config_parser.read(self.home_area()+"/config.ini")
	def save(self):
		self.config_parser.write(open(self.home_area()+"/config.ini", "w"))
		for backup in self.open_backups:
			# Save the data for the backup
			pass
	def close(self):
		#for backup in self.open_backups:
		#	backup.close()
		pass
	
	def create_backup(self, label):
		if self.has_backup(label):
			raise "Backup %s already exists"%label

		print "Creating backup label[%s]"%(label)
		backup = Backup.Backup(self, label)
		self.open_backups.append(backup)
		self.config_parser.add_section("backups/" + label)
		
		return backup
	def load_backup(self, label):
		if not self.has_backup(label):
			raise "Backup %s does not exist"%label
		
		backup = Backup.Backup(self, label)
		self.open_backups.append(backup)
		
		return backup
	def remove_backup(self, label):
		if not self.has_backup(label):
			raise "Backup %s does not exist"%label
		backup = Backup.Backup(self, label)
		backup.remove()
		
	def has_backup(self,label):
		return self.config_parser.has_section("backups/"+label)
	def list_backups(self):
		result = []
		for key in self.config_parser.sections():
			match = re.match("backups/([^/]+)", key)
			if match:
				result.append(match.group(1))
		return result
	def get_backup(self, label):
		return self.backups[label]


