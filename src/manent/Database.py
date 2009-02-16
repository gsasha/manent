#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import bsddb.db as db
import logging
import os
import os.path
import threading
import time
import traceback

import Config
import Reporting

class CheckpointThread(threading.Thread):
  def __init__(self, dbenv, done_event, checkpoint_finished):
    threading.Thread.__init__(self)
    self.dbenv = dbenv
    self.done_event = done_event
    self.checkpoint_finished = checkpoint_finished
  def run(self):
    #print "CHECKPOINT THREAD STARTED"
    while True:
      self.done_event.wait(60.0)
      if self.done_event.isSet():
        self.checkpoint_finished.set()
        break
      #print "RUNNING DATABASE CHECKPOINT"
      self.dbenv.txn_checkpoint(100, 5, 0)
      #print "DONE RUNNING DATABASE CHECKPOINT"


# A base for database managers, implementing the following common
# functionality:
# - Reporting
class DatabaseManagerBase:
  def __init__(self):
    self.num_puts_reporter = Reporting.DummyReporter()
    self.num_gets_reporter = Reporting.DummyReporter()
    self.num_dels_reporter = Reporting.DummyReporter()
    self.num_has_keys_reporter = Reporting.DummyReporter()
    self.report_manager = Reporting.DummyReportManager()
  def set_report_manager(self, report_manager):
    self.report_manager = report_manager
    self.num_puts_reporter = report_manager.find_reporter(
        "database.total.put", 0)
    self.num_gets_reporter = report_manager.find_reporter(
        "database.total.get", 0)
    self.num_dels_reporter = report_manager.find_reporter(
        "database.total.del", 0)
    self.num_has_keys_reporter = report_manager.find_reporter(
        "database.total.has_key", 0)


# A database manager that operates in memory only.
# Used for unit testing
class PrivateDatabaseManager(DatabaseManagerBase):
  def __init__(self):
    DatabaseManagerBase.__init__(self)
    self.dbenv = db.DBEnv()
    self.dbenv.set_cachesize(0, 100*1024*1024, 1)
    temp_area = Config.paths.temp_area().encode('utf8')
    self.dbenv.open(temp_area,
        db.DB_PRIVATE|db.DB_CREATE|db.DB_INIT_TXN|
        db.DB_INIT_MPOOL|db.DB_INIT_LOCK|db.DB_THREAD)
  def close(self):
    #
    # Close up the db environment. The user should have been
    # smart enough to close it himself.
    #
    self.dbenv.close()
    dbenv = db.DBEnv()
    self.dbenv = None
  def get_database(self, filename, tablename, txn_handler):
    name = filename + "." + str(tablename)
    d = DatabaseWrapper(self, name, name, txn_handler)
    d.set_report_manager(self.report_manager)
    return d
  def get_database_btree(self, filename, tablename, txn_handler):
    name = filename + "." + str(tablename)
    d = DatabaseWrapper(self, name, name, txn_handler, db_type=db.DB_BTREE)
    d.set_report_manager(self.report_manager)
    return d
  def get_database_hash(self, filename, tablename, txn_handler):
    name = filename + "." + str(tablename)
    d = DatabaseWrapper(self, name, name, txn_handler, db_type=db.DB_HASH)
    d.set_report_manager(self.report_manager)
    return d
  def get_scratch_database(self, filename, tablename):
    # Under Private database, the scratch database is no different from any
    # other, except that it has no transactions.
    name = filename + "." + str(tablename)
    return DatabaseWrapper(self, name, name)
  def txn_begin(self):
    return None
  def txn_checkpoint(self):
    pass

# The normal database manager class
class DatabaseManager(DatabaseManagerBase):
  def __init__(self, path_config, db_file_prefix):
    DatabaseManagerBase.__init__(self)
    self.path_config = path_config
    self.db_file_prefix = db_file_prefix
    
    self.open_dbs = {}
    self.scratch_dbs = {}

    #
    # Configure the database environment
    #
    self.dbenv = db.DBEnv()
    self.dbenv.set_cachesize(0, 50*1024*1024)
    self.dbenv.set_lk_max_locks(20000)
    self.dbenv.set_lk_max_objects(20000)
    self.dbenv.set_lk_detect(db.DB_LOCK_DEFAULT)
    if hasattr(db, "DB_LOG_AUTOREMOVE"):
      # This is the way it is defined in python up to 2.5
      self.dbenv.set_flags(db.DB_LOG_AUTOREMOVE, True)
    elif hasattr(db, "DB_LOG_AUTO_REMOVE"):
      # This is the way it is defined in python 2.6
      self.dbenv.set_flags(db.DB_LOG_AUTO_REMOVE, True)
    else:
      raise Exception("Can't set the database to auto remove logs")
    self.dbenv.set_flags(db.DB_TXN_WRITE_NOSYNC, True)
    self.dbenv.set_flags(db.DB_TXN_NOSYNC, True)
    self.dbenv.set_lg_bsize(2 * 1024 * 1024)
    #print "Opening environment in", self.__dbenv_dir()
    self.dbenv.open(self.__dbenv_dir(),
        db.DB_RECOVER|db.DB_CREATE|db.DB_INIT_TXN|
        db.DB_INIT_MPOOL|db.DB_INIT_LOCK|db.DB_THREAD)
    #print "dbenv.open() takes", (open_end_time-open_start_time), "seconds"
    
    #self.done_event = Event()
    #self.checkpoint_finished = Event()
    #self.checkpoint_thread = CheckpointThread(self.dbenv, self.done_event,
    #                                          self.checkpoint_finished)
    #self.checkpoint_thread.start()
  def txn_begin(self):
    self.dbenv.txn_checkpoint()
    return self.dbenv.txn_begin(flags=db.DB_DIRTY_READ)
  def txn_checkpoint(self):
    self.dbenv.txn_checkpoint()
  def close(self):
    #
    # Close up the db environment. The user should have been
    # smart enough to close it himself.
    #

    #
    # Free up the files that the database held
    #
    #self.done_event.set()
    #print "Waiting for the checkpoint thread to finish"
    #self.checkpoint_finished.wait()
    self.dbenv.close()
    self.dbenv = None
    dbenv = db.DBEnv()
    dbenv_dir = self.__dbenv_dir()
    dbenv.remove(dbenv_dir)

  def get_database(self, filename, tablename, txn_handler):
    full_fname = self.__db_fname(filename)
    d = DatabaseWrapper(self, full_fname, tablename, txn_handler)
    d.set_report_manager(self.report_manager)
    return d
  def get_database_btree(self, filename, tablename, txn_handler):
    full_fname = self.__db_fname(filename)
    d = DatabaseWrapper(self, full_fname, tablename, txn_handler,
      db_type=db.DB_BTREE)
    d.set_report_manager(self.report_manager)
    return d
  def get_database_hash(self, filename, tablename, txn_handler):
    full_fname = self.__db_fname(filename)
    d = DatabaseWrapper(self, full_fname, tablename, txn_handler,
      db_type=db.DB_HASH)
    d.set_report_manager(self.report_manager)
    return d
  def get_scratch_database(self, filename, tablename):
    full_fname = self.__scratch_db_fname(filename)
    assert tablename is None
    try:
      os.unlink(full_fname)
    except:
      # If the file doesn't exist, the better
      pass
    d = DatabaseWrapper(self, full_fname, tablename, txn_handler=None,
      is_scratch = True)
    d.set_report_manager(self.report_manager)
    return d
  def get_queue_database(self, filename, tablename):
    raise Exception("Not implemented")
  def remove_database(self, filename, tablename=None):
    #
    # Now actually delete the database file
    #
    full_fname = self.__db_fname(filename)
    d = db.DB(self.dbenv)
    d.remove(full_fname, tablename)
  def remove_scratch_database(self, filename, tablename=None):
    #
    # Now actually delete the database file
    #
    #print "Removing scratch database", filename, tablename
    fname = self.__scratch_db_fname(filename)
    d = db.DB(self.dbenv)
    d.remove(fname, tablename)
  
  def __dbenv_dir(self):
    home_area = self.path_config.backup_home_area(self.db_file_prefix).encode(
        'utf8')
    return home_area
  
  def __db_fname(self, filename):
    home_area = self.path_config.backup_home_area(self.db_file_prefix).encode(
        'utf8')
    result = os.path.join(home_area, filename)
    return result
  def __scratch_db_fname(self, filename):
    staging_area = self.path_config.backup_staging_area(
        self.db_file_prefix).encode('utf8')
    result = os.path.join(staging_area, filename)
    return result

class TransactionHandler:
  """
  Handles a single transaction context.
  Unlike a transaction context, a TransactionHandler object
  can be kept indefinitely, and remains valid even after a transaction
  is committed or aborted.
  """
  def __init__(self, db_manager):
    self.db_manager = db_manager
    self.txn = None
    class DummyReporter:
      def increment(self,value):
        pass
    self.commit_reporter = Reporting.DummyReporter()
    self.abort_reporter = Reporting.DummyReporter()
    self.checkpoint_reporter = Reporting.DummyReporter()

    self.precommit_hooks = []
  def set_report_manager(self, report_manager):
    self.commit_reporter = report_manager.find_reporter(
        "database.transactions.commits", 0)
    self.abort_reporter = report_manager.find_reporter(
        "database.transactions.aborts", 0)
    self.checkpoint_reporter = report_manager.find_reporter(
        "database.transactions.checkpoints", 0)

  def add_precommit_hook(self, hook):
    self.precommit_hooks.append(hook)
  def remove_precommit_hook(self, hook):
    self.precommit_hooks = [h for h in self.precommit_hooks
        if h is not hook]
  def get_txn(self):
    if self.txn is None:
      self.txn = self.db_manager.txn_begin()
    return self.txn
  def commit(self):
    #print "Committing transaction", self.txn
    for hook in self.precommit_hooks:
      hook()
    if self.txn is not None:
      self.commit_reporter.increment(1)
      self.txn.commit()
    self.txn = None
  def checkpoint(self):
    self.checkpoint_reporter.increment(1)
    self.db_manager.txn_checkpoint()
  def abort(self):
    if self.txn is not None:
      self.abort_reporter.increment(1)
      self.txn.abort()
    self.txn = None

class DatabaseWrapper:
  """
  Provides a Python Dictionary-like interface to a single database table.
  Objects of this class are meant to be created by db_manager, not by the user!
  """
  def __init__(self, db_manager, filename, dbname, txn_handler = None,
      is_scratch = False, db_type = db.DB_HASH):
    self.db_manager = db_manager
    self.filename = filename
    self.dbname = dbname
    self.txn_handler = txn_handler
    self.is_scratch = is_scratch
    
    self.d = db.DB(self.db_manager.dbenv)
    self.cursor = None
    
    self.num_puts_reporter = Reporting.DummyReporter()
    self.num_gets_reporter = Reporting.DummyReporter()
    self.num_dels_reporter = Reporting.DummyReporter()
    self.num_has_keys_reporter = Reporting.DummyReporter()

    logging.debug("Opening database filename=%s, dbname=%s" %
        (self.__get_filename(), self.__get_dbname()))
    start = time.time()
    self.d.set_pagesize(64*1024)
    self.d.open(self.__get_filename(), self.__get_dbname(), db_type,
        db.DB_CREATE, txn=self.__get_txn())
    end = time.time()
    logging.debug("Opening took %f seconds" % (end - start))

  def set_report_manager(self, report_manager):
    self.report_manager = report_manager
    self.num_puts_reporter = report_manager.find_reporter(
        "database.%s.%s.put" % (self.filename, self.dbname),
        0)
    self.num_gets_reporter = report_manager.find_reporter(
        "database.%s.%s.get" % (self.filename, self.dbname),
        0)
    self.num_dels_reporter = report_manager.find_reporter(
        "database.%s.%s.del" % (self.filename, self.dbname),
        0)
    self.num_has_keys_reporter = report_manager.find_reporter(
        "database.%s.%s.has_key" % (self.filename, self.dbname),
        0)

  def __get_filename(self):
    return self.filename
  def __get_dbname(self):
    return self.dbname
  def __get_txn(self):
    if self.txn_handler is None:
      return None
    return self.txn_handler.get_txn()
  #
  # Access methods
  #
  def get(self, key):
    #print "db[%s:%s].get(%s)" % (self.filename,self.dbname,i
    #   base64.b64encode(key[0:10]))
    txn = self.__get_txn()
    self.db_manager.num_gets_reporter.increment(1)
    self.num_gets_reporter.increment(1)
    return self.d.get(str(key), txn=txn)
  def put(self, key, value):
    #print "db[%s:%s].put(%s,%s)" % (self.filename,self.dbname,
    #  base64.b64encode(key[0:10]), base64.b64encode(value[0:10]))
    self.db_manager.num_puts_reporter.increment(1)
    self.num_puts_reporter.increment(1)
    self.d.put(str(key), str(value), txn=self.__get_txn())
  def __getitem__(self,key):
    #print "db[%s:%s].get(%s)" % (self.filename,self.dbname,
    # base64.b64encode(key[0:10]))
    return self.get(key)
  def __setitem__(self, key, value):
    logging.debug("db[%s:%s].set(%s,%s)" %
        (self.filename, self.dbname,
          base64.b64encode(key),
          base64.b64encode(value[0:10])))
    #traceback.print_stack()
    return self.put(key, value)
  def __delitem__(self, key):
    #print "db[%s:%s].del(%s)" % (self.filename,self.dbname,
    # base64.b64encode(key[0:10]))
    self.db_manager.num_dels_reporter.increment(1)
    self.num_dels_reporter.increment(1)
    self.d.delete(key, txn=self.__get_txn())
  def __len__(self):
    stat = self.d.stat()
    return stat['ndata']
  def has_key(self, key):
    #print "db[%s:%s].has_key(%s)" % (self.filename,self.dbname,
    # base64.b64encode(key[0:10]))
    self.db_manager.num_has_keys_reporter.increment(1)
    self.num_has_keys_reporter.increment(1)
    return self.d.get(str(key), txn=self.__get_txn()) != None
  #
  # Database cleanup options
  #
  def truncate(self):
    self.d.truncate(txn=self.__get_txn())
  #
  # Transaction support
  #
  def close_cursor(self):
    if self.cursor != None:
      self.cursor.close()
      self.cursor = None
  def close(self):
    #traceback.print_stack()
    logging.debug("Closing database filename=%s, dbname=%s" %
        (self.__get_filename(), self.__get_dbname()))
    #assert self.filename is not None
    self.d.close()
    self.d = None
    if self.is_scratch:
      self.db_manager.remove_scratch_database(self.filename)
    self.filename = None
  #
  # Iteration support
  #
  class Iter:
    def __init__(self, cursor, reporter_closure):
      self.cursor = cursor
      self.reporter_closure = reporter_closure
    def __iter__(self):
      return self
    def next(self):
      if self.rec is None:
        self.stop_iteration()
      try:
        self.reporter_closure()
        rec = self.rec
        self.rec = self.cursor.next()
        return rec
      except:
        traceback.print_exc()
        self.stop_iteration()
    def stop_iteration(self):
      self.cursor.close()
      self.cursor = None
      raise StopIteration
  class AllIter(Iter):
    def __init__(self, cursor, reporter_closure):
      DatabaseWrapper.Iter.__init__(self, cursor, reporter_closure)
      self.rec = cursor.first()
  class PrefixIter(Iter):
    def __init__(self, cursor, prefix, reporter_closure):
      DatabaseWrapper.Iter.__init__(self, cursor, reporter_closure)
      self.rec = cursor.set_range(prefix)
      self.prefix = prefix
    def next(self):
      k, v = DatabaseWrapper.Iter.next(self)
      if not k.startswith(self.prefix):
        raise StopIteration
      return (k, v)
  class KeysIter:
    def __init__(self, it):
      self.it = it
    def __iter__(self):
      return self
    def next(self):
      k, v = self.it.next()
      return k

  #def __iter__(self):
    #return self.get_all()
  def reporter_closure(self):
    self.num_gets_reporter.increment(1)
    self.db_manager.num_gets_reporter.increment(1)
  def iteritems(self):
    return DatabaseWrapper.AllIter(self.d.cursor(self.__get_txn()),
        self.reporter_closure)
  def iterkeys(self):
    return DatabaseWrapper.KeysIter(self.iteritems(),
        self.reporter_closure)
  def itervalues(self):
    pass
  #
  # Iteration over a subset of keys
  #
  def iteritems_prefix(self, prefix):
    return DatabaseWrapper.PrefixIter(self.d.cursor(self.__get_txn()),
        prefix, self.reporter_closure)
  def iterkeys_prefix(self, prefix):
    pass
  def itervalues_prefix(self, prefix):
    pass

