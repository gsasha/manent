import os, os.path
import bsddb
from bsddb import db
import base64

class DatabaseConfig:
	def __init__(self,global_config,filename):
		self.global_config = global_config
		self.filename = filename
		
		self.open_dbs = {}
		self.scratch_dbs = {}

		#
		# Configure the database environment
		#
		self.dbenv = db.DBEnv()
		self.dbenv.set_cachesize(0,100*1024*1024)
		self.dbenv.set_lk_max_locks(20000)
		self.dbenv.set_lk_max_objects(20000)
		self.dbenv.set_lk_detect(db.DB_LOCK_DEFAULT)
		self.dbenv.set_flags(db.DB_LOG_AUTOREMOVE, True)
		self.dbenv.open(self.__dbenv_dir(), db.DB_RECOVER| db.DB_CREATE |db.DB_INIT_TXN| db.DB_INIT_MPOOL| db.DB_INIT_LOCK|db.DB_THREAD)

	def txn_begin(self):
		self.dbenv.txn_checkpoint()
		return self.dbenv.txn_begin(flags=db.DB_DIRTY_READ)
	
	def close(self):
		#
		# Close up the db environment. The user should have been
		# smart enough to close it himself.
		#
		#self.close_cursors()
		#self.close_dbs()
		#if self.txn:
			#self.txn.abort()
			#self.txn = None

		#
		# Free up the files that the database held
		#
		self.dbenv.close()
		dbenv = db.DBEnv()
		dbenv.remove(self.__dbenv_dir())

	def get_database(self,tablename,txn_handler):
		fname = self.__db_fname(tablename)
		return DatabaseWrapper(self, fname, tablename, txn_handler)
	def get_scratch_database(self,tablename):
		fname = self.__scratch_db_fname(tablename)
		return DatabaseWrapper(self, fname, tablename, txn_handler=None)
	def get_queue_database(self,tablename):
		raise "Not implemented"
	def remove_database(self,tablename=None):
		#
		# Now actually delete the database file
		#
		fname = self.__db_fname(tablename)
		d = db.DB(self.dbenv)
		print "Removing database", self.filename, tablename
		d.remove(fname,tablename)
	def remove_scratch_database(self,tablename=None):
		#
		# Now actually delete the database file
		#
		fname = self.__scratch_db_fname(tablename)
		d = db.DB(self.dbenv)
		print "Removing scratch database", self.filename, tablename
		d.remove(fname,tablename)
	
	def __dbenv_dir(self):
		home_area = self.global_config.home_area()
		return home_area
	
	def __db_fname(self,tablename):
		return os.path.join(self.global_config.home_area(),self.filename)
	def __scratch_db_fname(self,tablename):
		return os.path.join(self.global_config.staging_area(),self.filename)

class TransactionHandler:
	"""
	Handles a single transaction context.
	Unlike a transaction context, a TransactionHandler object
	can be kept indefinitely, and remains valid even after a transaction
	is committed or aborted.
	"""
	def __init__(self,db_config):
		self.db_config = db_config
		self.txn = None
	def get_txn(self):
		if self.txn is None:
			self.txn = self.db_config.txn_begin()
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
	Objects of this class are meant to be created by db_config, not by the user!
	"""
	def __init__(self,db_config,filename,dbname,txn_handler=None):
		self.db_config = db_config
		self.filename = filename
		self.dbname = dbname
		self.txn_handler = txn_handler
		
		self.d = db.DB(self.db_config.dbenv)
		self.cursor = None
		
		#print "Opening database filename=%s, dbname=%s" %(self.__get_filename(),self.__get_dbname())
		self.d.open(self.__get_filename(), self.__get_dbname(), db.DB_HASH, db.DB_CREATE, txn=self.__get_txn())

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
	def put(self,key,value):
		#print "db[%s:%s].put(%s,%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]), base64.b64encode(value[0:10]))
		self.d.put(str(key),str(value),txn=self.__get_txn())
	def __getitem__(self,key):
		#print "db[%s:%s].get(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		return self.get(key)
	def __setitem__(self,key,value):
		#print "db[%s:%s].set(%s,%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]), base64.b64encode(value[0:10]))
		return self.put(key,value)
	def __delitem__(self,key):
		#print "db[%s:%s].del(%s)" % (self.filename,self.dbname, base64.b64encode(key[0:10]))
		self.d.delete(key,txn=self.__get_txn())
	def __len__(self):
		stat = self.d.stat()
		return stat['ndata']
	def has_key(self,key):
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
		print "Closing database filename=%s, dbname=%s" %(self.__get_filename(),self.__get_dbname())
		self.d.close()
		self.d = None
		self.filename=None
	#
	# Iteration support
	#
	def __iter__(self):
		self.cursor = self.d.cursor(self.__get_txn())
		if self.cursor.first() == None:
			self.cursor.close()
			self.cursor = None
		else:
			self.last_key = None
		return self
	def next(self):
		if self.cursor == None:
			raise StopIteration
		(key,value) = self.cursor.current()
		if key == self.last_key:
			self.cursor.close()
			self.cursor = None
		else:
			self.last_key = key
			self.cursor.next()
		return (key,value)

