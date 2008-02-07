#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import os, os.path
import bsddb.db as db
import base64
import time
import threading
import traceback

import Config

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

# A database manager that operates in memory only.
# Used for unit testing
class PrivateDatabaseManager:
	def __init__(self):
		self.dbenv = db.DBEnv()
		self.dbenv.open("/tmp", db.DB_PRIVATE|db.DB_CREATE|db.DB_INIT_TXN|
						db.DB_INIT_MPOOL|db.DB_INIT_LOCK|db.DB_THREAD)
	def get_database(self, filename, tablename, txn_handler):
		return DatabaseWrapper(self, None, filename + "." + str(tablename),
			txn_handler)
	def get_database_btree(self, filename, tablename, txn_handler):
		return DatabaseWrapper(self, None, filename + "." + str(tablename),
			txn_handler, db_type=db.DB_BTREE)
	def get_database_hash(self, filename, tablename, txn_handler):
		return DatabaseWrapper(self, None, filename + "." + str(tablename),
			txn_handler, db_type=db.DB_HASH)
	def get_scratch_database(self, filename, tablename):
		# Under Private database, the scratch database is no different from any other,
		# except that it has no transactions
		return DatabaseWrapper(self, None, filename + "." + str(tablename))
	def txn_begin(self):
		return None

# The normal database manager class
class DatabaseManager:
	def __init__(self, global_config, db_file_prefix):
		self.global_config = global_config
		self.db_file_prefix = db_file_prefix
		
		self.open_dbs = {}
		self.scratch_dbs = {}

		#
		# Configure the database environment
		#
		self.dbenv = db.DBEnv()
		self.dbenv.set_cachesize(0, 100*1024*1024)
		self.dbenv.set_lk_max_locks(20000)
		self.dbenv.set_lk_max_objects(20000)
		self.dbenv.set_lk_detect(db.DB_LOCK_DEFAULT)
		self.dbenv.set_flags(db.DB_LOG_AUTOREMOVE, True)
		print "Opening environment in", self.__dbenv_dir()
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
		dbenv = db.DBEnv()
		dbenv.remove(self.__dbenv_dir())

	def get_database(self, filename, tablename, txn_handler):
		full_fname = self.__db_fname(filename)
		return DatabaseWrapper(self, full_fname, tablename, txn_handler)
	def get_database_btree(self, filename, tablename, txn_handler):
		full_fname = self.__db_fname(filename)
		return DatabaseWrapper(self, full_fname, tablename, txn_handler,
			db_type=db.DB_BTREE)
	def get_database_hash(self, filename, tablename, txn_handler):
		full_fname = self.__db_fname(filename)
		return DatabaseWrapper(self, full_fname, tablename, txn_handler,
			db_type=db.DB_HASH)
	def get_scratch_database(self, filename, tablename):
		full_fname = self.__scratch_db_fname(filename)
		assert tablename is None
		try:
			os.unlink(full_fname)
		except:
			# If the file doesn't exist, the better
			pass
		return DatabaseWrapper(self, full_fname, tablename, txn_handler=None,
			is_scratch = True)
	def get_queue_database(self, filename, tablename):
		raise Exception("Not implemented")
	def remove_database(self, filename, tablename=None):
		#
		# Now actually delete the database file
		#
		full_fname = self.__db_fname(filename)
		d = db.DB(self.dbenv)
		print "Removing database", filename, tablename
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
		home_area = Config.paths.backup_home_area(self.db_file_prefix)
		return home_area
	
	def __db_fname(self, filename):
		return os.path.join(
			Config.paths.backup_home_area(self.db_file_prefix), filename)
	def __scratch_db_fname(self, filename):
		return os.path.join(
			Config.paths.backup_staging_area(self.db_file_prefix),
			filename)

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
	def get_txn(self):
		if self.txn is None:
			self.txn = self.db_manager.txn_begin()
		return self.txn
	def commit(self):
		if self.txn is not None:
			self.txn.commit()
		self.txn = None
	def abort(self):
		if self.txn is not None:
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
		
		#print "Opening database filename=%s, dbname=%s" %(self.__get_filename(), self.__get_dbname())
		start = time.time()
		self.d.open(self.__get_filename(), self.__get_dbname(), db_type, db.DB_CREATE, txn=self.__get_txn())
		end = time.time()
		#print "opening database %s:%s takes %f seconds" % (self.__get_filename(),self.__get_dbname(),end-start)

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
	def get(self,key):
		#print "db[%s:%s].get(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		txn = self.__get_txn()
		return self.d.get(str(key), txn=txn)
	def put(self, key, value):
		#print "db[%s:%s].put(%s,%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]), base64.b64encode(value[0:10]))
		self.d.put(str(key), str(value), txn=self.__get_txn())
	def __getitem__(self,key):
		#print "db[%s:%s].get(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		return self.get(key)
	def __setitem__(self, key, value):
		#print "db[%s:%s].set(%s,%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]), base64.b64encode(value[0:10]))
		return self.put(key, value)
	def __delitem__(self, key):
		#print "db[%s:%s].del(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		self.d.delete(key, txn=self.__get_txn())
	def __len__(self):
		stat = self.d.stat()
		return stat['ndata']
	def has_key(self, key):
		#print "db[%s:%s].has_key(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		return self.get(key) != None
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
		#print "Closing database filename=%s, dbname=%s" %(self.__get_filename(),self.__get_dbname())
		self.d.close()
		self.d = None
		if self.is_scratch:
			self.db_manager.remove_scratch_database(self.filename)
		self.filename = None
	#
	# Iteration support
	#
	class Iter:
		def __init__(self, cursor):
			self.cursor = cursor
		def __iter__(self):
			return self
		def next(self):
			if self.rec is None:
				self.cursor.close()
				self.cursor = None
				raise StopIteration
			try:
				rec = self.rec
				self.rec = self.cursor.next()
				return rec
			except:
				print "Ouch, there is some exception:"
				traceback.print_exc()
				self.cursor.close()
				self.cursor = None
				raise StopIteration
	class AllIter(Iter):
		def __init__(self, cursor):
			DatabaseWrapper.Iter.__init__(self, cursor)
			self.rec = cursor.first()
	class PrefixIter(Iter):
		def __init__(self, cursor, prefix):
			DatabaseWrapper.Iter.__init__(self, cursor)
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
	def iteritems(self):
		return DatabaseWrapper.AllIter(self.d.cursor(self.__get_txn()))
	def iterkeys(self):
		return KeysIter(self.iteritems())
	def itervalues(self):
		pass
	#
	# Iteration over a subset of keys
	#
	def iteritems_prefix(self, prefix):
		return DatabaseWrapper.PrefixIter(self.d.cursor(self.__get_txn()), prefix)
	def iterkeys_prefix(self, prefix):
		pass
	def itervalues_prefix(self, prefix):
		pass

