#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import os.path
import re
import sys
import tempfile
import time
import traceback

import CompletedNodesDB
import Config
import Container
import Database
import ExclusionProcessor
import IncrementManager
import Nodes
import Reporting
import StorageManager

def parse_to_keys(params):
  result = {}
  for kv in params:
    key, value = kv.split('=')
    result[key] = value
  return result

class Backup:
  """
  Database of a complete backup set
  """
  def __init__(self, global_config, label):
    self.global_config = global_config
    self.report_manager = Reporting.ReportManager()
    self.label = label
    self.storage_opened = False

    home_dir = Config.paths.backup_home_area(self.label)
    if not os.path.isdir(home_dir):
      os.makedirs(home_dir, 0700)
    staging_dir = Config.paths.backup_staging_area(self.label)
    if not os.path.isdir(staging_dir):
      os.makedirs(Config.paths.backup_staging_area(self.label), 0700)
    exclusion_file_name = os.path.join(home_dir, "exclusion_rules")
    if not os.path.isfile(exclusion_file_name):
      exclusion_file = open(exclusion_file_name, "w")
      exclusion_file.write(Config.BACKUP_EXCLUSION_RULES_TEMPLATE)
      exclusion_file.close()

  def get_report_manager(self):
    return self.report_manager
  def write_log(self):
    log_dir = Config.paths.backup_home_area(self.label)
    log_file_name = os.path.join(log_dir, "log.txt") 
    log_file = open(log_file_name, "a")
    log_file.write("Performing command:\n%s\n" % " ".join(sys.argv))
    log_detail_file, log_detail_file_name = tempfile.mkstemp(
        prefix="report-",
        dir=Config.paths.backup_home_area(self.label),
        suffix=".txt")
    log_file.write("Detailed report stored in file: %s\n" %
        log_detail_file_name)
    class FDFileWrapper:
      def __init__(self, fd):
        self.fd = fd
      def write(self, str):
        os.write(self.fd, str)
    self.report_manager.write_report(FDFileWrapper(log_detail_file))
    print "Added log entry to %s" % (
        log_file_name.encode("utf8"))
    print "Wrote detailed report to %s" % (
        log_detail_file_name.encode("utf8"))

  def close(self):
    self.txn_handler.close()
    self.txn_handler = None
    self.db_manager.close()
    self.db_manager = None

  #
  # Two initialization methods:
  # Creation of new backup, loading from live DB
  #
  def configure(self, args):
    try:
      self.__open_all()
      params = parse_to_keys(args[1:])
      if args[0] == 'show':
        for k, v in self.config_db.iteritems():
          print k, '=', v
      elif args[0] == 'set':
        for key in ['data_path']:
          if params.has_key(key):
            self.config_db[key] = params[key]
      elif args[0] == 'add_exclusion':
        exclusion_type = params['type']
        exclusion_action = params['action']
        exclusion_pattern = params['pattern']
        if self.config_db.has_key('num_exclusion_rules'):
          n = int(self.config_db['num_exclusion_rules'])
        else:
          n = 0
        if not exclusion_type in ['relative', 'absolute', 'wildcard']:
          raise Exception("Unknown rule type " + exclusion_type)
        if not exclusion_action in ['include', 'exclude']:
          raise Exception("Unknown rule action " + exclusion_action)
        self.config_db['exclusion_rule_%d.type' % n] = exclusion_type
        self.config_db['exclusion_rule_%d.action' % n] = exclusion_action
        self.config_db['exclusion_rule_%d.pattern' % n] = exclusion_pattern
        self.config_db['num_exclusion_rules'] = str(n + 1)
      elif args[0] == 'add_storage':
        self.__open_storage()
        storage_idx = self.storage_manager.add_storage(params)
        self.storage_manager.make_active_storage(storage_idx)
      elif args[0] == 'add_base_storage':
        self.__open_storage()
        self.storage_manager.add_storage(params)
      self.txn_handler.commit()
    finally:
      self.__close_all()
      self.write_log()

  def load(self):
    self.data_path = self.config_db["data_path"]
    self.storage_manager.load()
    self.exclusion_processor.load()
  
  def remove(self):
    print "Removing backup", self.label
    raise Exception("Not implemented")

  def create(self):
    try:
      logginng.debug("Create starting")
      self.__open_all()

      # Nothing to do here
      
      self.txn_handler.commit()
      logging.debug("Create finished. Now must close everything down")
    except:
      traceback.print_exc()
      self.txn_handler.abort()
      raise
    finally:
      logging.debug("Closing everything down after create")
      self.__close_all()
      self.write_log()

  #
  # Scanning (adding a new increment)
  #
  def scan(self, args):
    try:
      self.__open_all()
      self.__open_exclusion_processor()
      self.__open_storage()

      params = parse_to_keys(args)
      if params.has_key("comment"):
        comment = params["comment"]
      else:
        comment = ""

      last_fs_digest, last_fs_level, last_num_files =\
          self.increment_manager.start_increment(comment)
      root = Nodes.Directory(self, None,
          unicode(self.config_db['data_path'], 'utf8'))
      root.set_weight(1.0)
      ctx = ScanContext(self, root, self.report_manager)
      if last_num_files is not None:
        ctx.set_last_num_files(last_num_files)

      prev_num = (Nodes.NODE_TYPE_DIR, None, last_fs_digest, last_fs_level)
      root.scan(ctx, prev_num, self.exclusion_processor)

      logging.info("Diff from previous increments: %d out of %d" %
          (ctx.changed_nodes, ctx.total_nodes))
      
      # Upload the special data to the containers
      self.increment_manager.finalize_increment(root.get_digest(),
        root.get_level(), root.get_stats(), ctx.total_nodes)
      self.storage_manager.flush()
      
      self.txn_handler.commit()
      ctx.print_report()
    except:
      traceback.print_exc()
      self.txn_handler.abort()
      raise
    finally:
      logging.debug("Closing everythinng down after scan")
      self.__close_all()
      self.write_log()

  #
  # Restoring an increment to filesystem
  #
  def restore(self, args):
    try:
      self.__open_all()
      self.__open_storage()

      params = parse_to_keys(args)
      storage = int(params['storage'])
      idx = int(params['increment'])
      target = unicode(params['target'], 'utf8')
      
      increment = self.increment_manager.get_increment(storage, idx)
      root = Nodes.Directory(self, None, target)
      root.set_digest(increment.get_fs_digest())
      root.set_level(increment.get_fs_level())
      root.set_stats(increment.get_fs_stats())
      ctx = RestoreContext()
      root.restore(ctx)
      
      self.txn_handler.commit()
    except:
      traceback.print_exc()
      self.txn_handler.abort()
      raise
    finally:
      self.__close_all()
      self.write_log()
  
  #
  # Serving the filesystem as ftp
  #
  def serve(self, args):
    try:
      import FTPServer
      self.__open_all()
      self.__open_storage()

      params = parse_to_keys(args)
      if params.has_key('port'):
        port = int(params['port'])
      else:
        port = 2221
      logging.info("Serving FTP on port " + str(port))
      FTPServer.serve(self, port)
    except:
      traceback.print_exc()
      self.txn_handler.abort()
    finally:
      self.__close_all()
      self.write_log()
  #
  # Testing that an increment can be loaded
  #
  def test(self, args):
    try:
      self.__open_all()
      self.__open_storage()

      params = parse_to_keys(args)
      storage = int(params['storage'])
      idx = int(params['increment'])
      
      increment = self.increment_manager.get_increment(storage, idx)
      root = Nodes.Directory(self, None, u"")
      root.set_digest(increment.get_fs_digest())
      root.set_level(increment.get_fs_level())
      root.set_stats(increment.get_fs_stats())
      ctx = RestoreContext()
      root.test(ctx)
      
      self.txn_handler.commit()
    except:
      traceback.print_exc()
      self.txn_handler.abort()
      raise
    finally:
      self.__close_all()
      self.write_log()
  
  #
  # Information
  #
  def info(self, args):
    try:
      self.__open_all()
      self.__open_storage()

      detail = args[0]
      params = parse_to_keys(args[1:])

      if detail == 'increments':
        increments = self.increment_manager.get_increments()
        for storage, increment_idxs in increments.iteritems():
          print "Storage", storage, "has increments:", increment_idxs
          for idx in increment_idxs:
            increment = self.increment_manager.get_increment(storage, idx)
            print ('  increment %3d:%s machine: %s time:%s comment:%s' %
                (idx,
                base64.b64encode(increment.get_attribute('sequence_id')),
                increment.get_attribute('hostname'),
                time.ctime(float(increment.get_attribute('ctime'))),
                increment.get_attribute('comment')))
      elif detail == 'fs':
        increments = self.increment_manager.get_increments()
        for storage, increment_idxs in increments.iteritems():
          for idx in increment_idxs:
            increment = self.increment_manager.get_increment(storage, idx)
            print "  increment comment:", increment.get_attribute("comment")
            print "  increment fs     :", base64.b64encode(increment.get_fs_digest())
            print "  increment time   :", increment.get_attribute("ctime")
            root_path = unicode(self.config_db['data_path'], 'utf8')
            root = Nodes.Directory(self, None, root_path)
            root.set_digest(increment.get_fs_digest())
            root.set_level(increment.get_fs_level())
            root.set_stats(increment.get_fs_stats())
            root.list_files()
      # TODO:Print info on all the storages
      # TODO:Print info on all the increments
      
      self.txn_handler.commit()
    except:
      traceback.print_exc()
      self.txn_handler.abort()
      raise
    finally:
      self.__close_all()
      self.write_log()
  
  def get_block_size(self):
    return self.storage_manager.get_block_size()
  def add_block(self, digest, code, data):
    return self.storage_manager.add_block(digest, code, data)
  def load_block(self, digest):
    return self.storage_manager.load_block(digest)
  def get_block_code(self, digest):
    return self.storage_manager.get_block_code(digest)
  def get_completed_nodes_db(self):
    return self.completed_nodes_db

  def __open_all(self):
    logging.debug("Opening the databases")
    self.db_manager = Database.DatabaseManager(Config.paths, self.label)
    self.db_manager.set_report_manager(self.report_manager)
    self.txn_handler = Database.TransactionHandler(self.db_manager)
    self.txn_handler.set_report_manager(self.report_manager)
    logging.debug("Opening the databases done")

    logging.debug("Backup opening all")
    self.config_db = self.db_manager.get_database_btree(
        "config.db", "settings", self.txn_handler)
    self.completed_nodes_db = CompletedNodesDB.CompletedNodesDB(
        self.db_manager, self.txn_handler)
    self.completed_nodes_db.load()
    #self.storage_manager = StorageManager.StorageManager(self.db_manager,
    #  self.txn_handler)
    #print "DATA PATH", self.config_db['data_path']

  def __open_exclusion_processor(self):
    self.exclusion_processor = ExclusionProcessor.ExclusionProcessor(
      unicode(self.config_db['data_path'], 'utf8'))
    
    # Function that imports the rule into the rules processor
    def process_rule(type_str, action_str, pattern):
      if action_str == 'exclude':
        action = ExclusionProcessor.RULE_EXCLUDE
      elif action_str == 'include':
        action = ExclusionProcessor.RULE_INCLUDE
      else:
        raise Exception()
      if type_str == 'absolute':
        self.exclusion_processor.add_absolute_rule(action, pattern)
      elif type_str == 'relative':
        self.exclusion_processor.add_rule(action, pattern)
      elif type_str == 'wildcard':
        self.exclusion_processor.add_wildcard_rule(action, pattern)
    def read_exclusion_file(file_name):
      if os.path.isfile(file_name):
        exclusion_rules_file = open(file_name, "r")
        for line in exclusion_rules_file:
          # Ignore comments and empty lines
          line = line.strip()
          if line.startswith("#"):
            continue
          if line == "":
            continue
          type_str, action_str, pattern = line.split(None, 2)
          process_rule(type_str, action_str, pattern)
    #
    # Read exclusion rules from the manent home dir
    #
    read_exclusion_file(os.path.join(Config.paths.home_area(),
      "exclusion_rules"))
    read_exclusion_file(os.path.join(
      Config.paths.backup_home_area(self.label),
      "exclusion_rules"))
    #
    # Process rules from the backup's db
    #
    if self.config_db.has_key('num_exclusion_rules'):
      num_exclusion_rules = int(self.config_db['num_exclusion_rules'])
      for r in range(num_exclusion_rules):
        type_str = self.config_db['exclusion_rule_%d.type' % r]
        action_str = self.config_db['exclusion_rule_%d.action' % r]
        pattern = self.config_db['exclusion_rule_%d.pattern' % r]
        process_rule(type_str, action_str, pattern)
  def __open_storage(self):
    # TODO: consider not loading storages on initialization, only on meaningful
    # operations
    logging.debug("Opening storage")
    self.storage_manager = StorageManager.StorageManager(
        self.db_manager, self.txn_handler)
    self.storage_manager.set_report_manager(self.report_manager)
    self.increment_manager = IncrementManager.IncrementManager(
      self.db_manager, self.txn_handler, self.label, self.storage_manager)
    self.storage_manager.load_storages()
    self.storage_opened = True
  def __close_all(self):
    if self.storage_opened:
      self.increment_manager.close()
      self.storage_manager.close()
      # In Integration test, we use the same backup object several times.
      self.storage_opened = False
    else:
      logging.debug("Storage has not been opened")
    self.completed_nodes_db.close()
    self.config_db.close()
    self.txn_handler.commit()
  
#===============================================================================
# ScanContext
#===============================================================================
class ScanContext:
  def __init__(self, backup, root_node, report_manager):

    self.inodes_db = {}

    self.total_nodes = 0
    self.changed_nodes = 0
    self.scan_timestamp = 0
    self.report_manager = report_manager

    self.last_num_files = None
    self.start_time = time.time()

    self.cur_container_sequence_id = ""
    self.cur_container_idx = ""
    self.cur_container_key = ""
    self.cur_container_size = ""

    self.num_new_containers = 0
    self.size_new_containers = 0
    self.num_downloaded_containers = 0
    self.size_downloaded_containers = 0

    self.num_visited_files_reporter = report_manager.find_reporter(
        "scan.counts.visited_files", 0)
    self.num_visited_dirs_reporter = report_manager.find_reporter(
        "scan.counts.visited_dirs", 0)
    self.num_visited_symlinks_reporter = report_manager.find_reporter(
        "scan.counts.visited_symlinks", 0)
    self.num_scanned_files_reporter = report_manager.find_reporter(
        "scan.counts.scanned_files", 0)
    self.num_scanned_dirs_reporter = report_manager.find_reporter(
        "scan.counts.scanned_dirs", 0)
    self.num_scanned_symlinks_reporter = report_manager.find_reporter(
        "scan.counts.scanned_symlinks", 0)
    self.num_prev_files_reporter = report_manager.find_reporter(
        "scan.counts.prev_files", 0)
    self.num_prev_symlinks_reporter = report_manager.find_reporter(
        "scan.counts.prev_symlinks", 0)
    self.num_prev_dirs_reporter = report_manager.find_reporter(
        "scan.counts.prev_dirs", 0)
    self.num_changed_files_reporter = report_manager.find_reporter(
        "scan.counts.changed_files", 0)
    self.num_changed_symlinks_reporter = report_manager.find_reporter(
        "scan.counts.changed_symlinks", 0)
    self.num_changed_dirs_reporter = report_manager.find_reporter(
        "scan.counts.changed_dirs", 0)

    self.changed_files_reporter = report_manager.find_reporter(
        "scan.changed_files", [])
    self.changed_dirs_reporter = report_manager.find_reporter(
        "scan.changed_dirs", [])
    self.changed_symlinks_reporter = report_manager.find_reporter(
        "scan.changed_symlinks", [])

    self.num_total_blocks_reporter = report_manager.find_reporter(
        "scan.counts.total_blocks", 0)
    self.size_total_blocks_reporter = report_manager.find_reporter(
        "scan.counts.size_total_blocks", 0)
    self.num_new_blocks_reporter = report_manager.find_reporter(
        "scan.counts.new_blocks", 0)
    self.size_new_blocks_reporter = report_manager.find_reporter(
        "scan.counts.size_new_blocks", 0)

    self.unrecognized_files_reporter = report_manager.find_reporter(
        "scan.warnings.unrecognized_files", [])
    self.oserror_files_reporter = report_manager.find_reporter(
        "scan.warnings.oserror_files", [])
    self.ioerror_files_reporter = report_manager.find_reporter(
        "scan.warnings.ioerror_files", [])

    self.current_scanned_file_reporter = report_manager.find_reporter(
        "scan.current_file", "")

    report_manager.add_listener(
        Reporting.CallbackListener("container.progress",
          self.update_container_status))
    report_manager.add_listener(
        Reporting.CallbackListener("storage",
          self.update_start_container))

    self.root_node = root_node
  def print_report(self):
    self.report_manager.set(
        "scanning.total_time",
        time.time() - self.start_time)
    print ("Backup done in %2.3f sec."
        "Visited %d files, scanned %d, found %d changed." % (
          time.time() - self.start_time,
          self.num_visited_files_reporter.value,
          self.num_scanned_files_reporter.value,
          self.num_changed_files_reporter.value))
    # TODO(gsasha): collect stats on the containers.
    print "Created %d new blocks, for total of %d bytes." % (
        0, 0)
    print "Created %d new containers, for total of %d bytes." % (
        self.num_new_containers,
        self.size_new_containers)
    if (self.unrecognized_files_reporter.value != [] or
        self.oserror_files_reporter.value != [] or
        self.ioerror_files_reporter.value != []):
      print "Problems detected in the following files:"
      for f in self.unrecognized_files_reporter.value:
        print "  %s: file type not recognized" % f.encode('utf8')
      for f in self.oserror_files_reporter.value:
        print "  %s: OSError accessing file" % f.encode('utf8')
      for f in self.ioerror_files_reporter.value:
        print "  %s: IOError accessing file" % f.encode('utf8')

  def set_last_num_files(self, last_num_files):
    self.last_num_files = last_num_files
  def clip_to_width(self, message, width):
    if len(message) < width:
      return message
    left_chunk_size = width / 2 - 3
    right_chunk_size = width - left_chunk_size
    return message[:left_chunk_size] + "..." + message[-right_chunk_size:]
  def format_filesize(self, size):
    if size < 1024:
      return "%d" % size
    elif size < 1024 * 1024:
      return "%dK" % (size / 1024.)
    elif size < 1024 * 1024 * 1024:
      return "%.2fM" % (size / (1024 * 1024.))
    else:
      return "%.3fG" % (size/ (1024 * 1024 * 1024.))

  def print_update_message(self, message):
    if self.last_num_files is not None and self.last_num_files != 0:
      progress = "%2.3f%%" % (self.num_visited_files_reporter.value * 100.0 /
          self.last_num_files)
    else:
      progress = "%5d" % self.num_visited_files_reporter.value
    elapsed = "%2.3f" % (time.time() - self.start_time)
    report_string = "%s sec:%s blk:%d/%s nblk:%d/%s %-80s      \r" % (
        elapsed,
        progress,
        self.num_total_blocks_reporter.value,
        self.format_filesize(self.size_total_blocks_reporter.value),
        self.num_new_blocks_reporter.value,
        self.format_filesize(self.size_new_blocks_reporter.value),
        self.clip_to_width(message, 80).encode('utf8'))
    try:
      sys.stderr.write(report_string)
    except:
      print "Cannot print status message"
  def update_scan_status(self):
    timestamp = time.time()
    if timestamp - self.scan_timestamp > 0.1:
      # Print updates at most 10 times per second
      self.print_update_message(self.current_scanned_file_reporter.value)
      self.scan_timestamp = timestamp
  def update_start_container(self, name, value):
    if name.startswith("storage.container.upload"):
      storage, container, upload, sequence_id, idx, key = name.split(".")
      self.cur_container_sequence_id = sequence_id
      self.cur_container_idx = idx
      self.cur_container_key = key
      self.cur_container_operation = "upload"
      self.cur_container_size = value
      self.cur_container_start_time = time.time()
      if name.endswith(".size"):
        self.num_new_containers += 1
        self.size_new_containers += int(self.cur_container_size)
    elif name.startswith("storage.container.download"):
      storage, container, download, sequence_id, idx, why, cmd = name.split(".")
      self.cur_container_sequence_id = sequence_id
      self.cur_container_idx = idx
      self.cur_container_operation = "download for " + why
      self.cur_container_size = value
      self.cur_container_start_time = time.time()
  def update_container_status(self, name, progress):
    speed = progress / 1024.0 / (time.time() + 1e-100 -
        self.cur_container_start_time)
    self.print_update_message("%s %s.%s: done %d of %s, speed: %2.2f KBps" %
        (self.cur_container_operation,
          self.cur_container_sequence_id,
          self.cur_container_idx,
          progress,
          self.cur_container_size,
          speed))

#=========================================================
# RestoreContext
#=========================================================
class RestoreContext:
  def __init__(self):
    self.inodes_db = {}
