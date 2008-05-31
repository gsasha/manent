#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import fnmatch
import logging
import os
import os.path
import re
import shutil
import sys
import tempfile

import ConfigParser
import Backup
import Container
import manent.utils.IntegerEncodings as IntegerEncodings

VERSION = "0.11.0"

print "This is Manent version %s" % VERSION

EXCLUSION_RULES_DOC = """

# Exclusion rules come in the form:
# TYPE ACTION PATTERN
# TYPE is one of ["absolute", "relative", "wildcard"]
# - absolute rules assume absolute path, i.e., are relative to the
#   root of the filesystem.
# - relative rules are relative to the root of the root directory
#   of the current backup.
# - wildcard rules operate on a single path element anywhere in the
#   directory tree.
#
# ACTION is one of ["include", "exclude"].
#   Note that if several rules are specified, the later ones override
#   the earlier ones.
#
# PATTERN is the path pattern, using "*" and "?" as wildcards.
# The "/" character can serve as a separator also on Windows, e.g.:
# c:/Temp/*

"""

EXCLUSION_RULES_TEMPLATE = """
# Exclusion rules file. Add your global exclusion rules here.
# This file is designed for manual editing and reading in by
# manent on startup.
""" + EXCLUSION_RULES_DOC + """
# IMPORTANT RULES:
# Do not back up manent's own directory
absolute exclude /home/*/.manent1

# EXAMPLES:

# Exclude cache directories frequently used under Linux:
#absolute exclude /home/*/.mozilla/firefox*/*/Cache
#absolute exclude /home/*/.local/share/Trash
#absolute exclude /home/*/.thumbnails
#absolute exclude /home/*/.google/desktop/repo

# Exclude backup files:
# wildcard exclude *~
"""

BACKUP_EXCLUSION_RULES_TEMPLATE = """
# Exclusion rules file. Add exclusion rules specific to this
# backup.
# This file is designed for manual editing and reading in by
# manent on startup.
""" + EXCLUSION_RULES_DOC + """
# EXAMPLES:

# Backup only the photos and mails from your current backup directory:
#relative exclude *
#relative include Photos
#relative include Mail
"""

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
    self.temp_area_path = None
  def home_area(self):
    if os.environ.has_key("MANENT_HOME_DIR"):
      # Allow the user to override the placement of
      # manent home, esp. for testing
      path = os.environ["MANENT_HOME_DIR"]
    elif os.name == "nt":
      path = os.path.join(os.environ["APPDATA"], "manent1")
    else:
      path = os.path.join(os.environ["HOME"], ".manent1")
    if not self.home_area_exists and not os.path.exists(path):
      os.makedirs(path, 0700)
      self.home_area_exists = True
      rules_file_name = os.path.join(path, "exclusion_rules")
      if not os.path.isfile(rules_file_name):
        print "Creating default rules file"
        rules_file = open(rules_file_name, "w")
        rules_file.write(EXCLUSION_RULES_TEMPLATE)
        rules_file.close()
    return path

  def backup_home_area(self, label):
    return os.path.join(self.home_area(), "BACKUP-" + label)

  def staging_area(self):
    if os.name == "nt":
      path = os.path.join(os.environ["TEMP"], "manent.staging")
    else:
      path = "/tmp/manent.staging."+os.environ["USER"]
    if not self.staging_area_exists and not os.path.exists(path):
      os.makedirs(path, 0700)
      self.staging_area_exists = True
    return path

  def backup_staging_area(self, label):
    return os.path.join(self.staging_area(), "BACKUP-" + label)

  def temp_area(self):
    if self.temp_area_path is not None:
      try:
        os.mkdir(self.temp_area_path)
      except:
        pass
      return self.temp_area_path
    if os.name == "nt":
      self.temp_area_path = tempfile.mkdtemp()
    else:
      self.temp_area_path = tempfile.mkdtemp(
          prefix=os.path.join("/tmp", ""))
    return self.temp_area_path
  def clean_temp_area(self):
    shutil.rmtree(self.temp_area())

paths = Paths()

def init_logging():
  import logging.config
  logging.basicConfig(
      format="%(relativeCreated)d:%(levelname)-8s:%(message)s")
  try:
    logging.config.fileConfig(os.path.join(paths.home_area(),
      "manent_logging_config"))
  except:
    pass
  try:
    logging.config.fileConfig("./.manent_logging_config")
  except:
    pass
  #print "Logging initialized"
  if os.environ.has_key("MANENT_LOGGING_LEVEL"):
    level = os.environ["MANENT_LOGGING_LEVEL"]
    LEVELS = { "NOTSET": logging.NOTSET,
           "DEBUG": logging.DEBUG,
           "INFO": logging.INFO,
           "ERROR": logging.ERROR,
           "CRITICAL": logging.CRITICAL }
    if LEVELS.has_key(level):
      logging.getLogger("").setLevel(LEVELS[level])
      logging.info("Setting logging level to " + level)
    else:
      print "Bad logging level env: MANENT_LOGGING_LEVEL=%s", level

init_logging()

class GlobalConfig:
  def __init__(self):
    self.config_parser = ConfigParser.ConfigParser()
    self.open_backups = []

  #
  # Configuration persistence
  #
  def load(self):
    self.config_parser.read(os.path.join(paths.home_area(),
      "config.ini"))
  def save(self):
    self.config_parser.write(open(os.path.join(paths.home_area(),
      "config.ini"), "w"))
    for backup in self.open_backups:
      # Save the data for the backup
      pass
  def close(self):
    #for backup in self.open_backups:
    # backup.close()
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


