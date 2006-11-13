import os, os.path
import bsddb
from bsddb import db

import Config

class DatabaseConfig:
	def __init__(self,global_config):
		self.global_config = global_config
		
		self.open_dbs = {}
		self.scratch_dbs = {}

		#
		# Make sure that directories in which database lives
		# do exist
		#
		#home_area = self.global_config.home_area()
		#if not os.path.isdir(home_area):
			#os.mkdir(home_area)
		#dbenv_dir = os.path.join(home_area,"/dbenv")
		#if not os.path.isdir(dbenv_dir):
			#os.mkdir(dbenv_dir)

		#
		# Configure the database environment
		#
		self.dbenv = db.DBEnv()
		self.dbenv.set_cachesize(0,100*1024*1024)
		self.dbenv.set_lk_max_locks(20000)
		self.dbenv.set_lk_max_objects(20000)
		self.dbenv.set_lk_detect(db.DB_LOCK_DEFAULT)
		#self.dbenv.open(dbenv_dir, db.DB_RECOVER| db.DB_CREATE |db.DB_INIT_TXN| db.DB_INIT_MPOOL| db.DB_INIT_LOCK)
		self.dbenv.open(self.dbenv_dir(), db.DB_RECOVER| db.DB_CREATE |db.DB_INIT_TXN| db.DB_INIT_MPOOL| db.DB_INIT_LOCK)

		self.txn = self.dbenv.txn_begin(flags=db.DB_TXN_NOSYNC|db.DB_DIRTY_READ|db.DB_TXN_NOWAIT)
		print "Started transaction", self.txn

	def dbenv_dir(self):
		home_area = self.global_config.home_area()
		return home_area
	
	def close(self):
		#
		# Close up the db environment. The user should have been
		# smart enough to close it himself.
		#
		self.close_cursors()
		self.close_dbs()
		if self.txn:
			self.txn.abort()
			self.txn = None
		
		#
		# Clean up the log files
		#
		for name in self.dbenv.log_archive():
			file = os.path.join(self.dbenv_dir(),name)
			os.unlink(file)

		#
		# Free up the files that the database held
		#
		self.dbenv.close()
		dbenv = db.DBEnv()
		dbenv.remove(self.dbenv_dir())

	def database_exists(self,name,tablename):
		fname = self.db_fname(name,tablename)
		return os.path.isfile(fname)
	def get_database(self,name,tablename):
		key = (name,tablename)
		if self.open_dbs.has_key(key):
			return self.open_dbs[key]
		
		fname = self.db_fname(name,tablename)
		d = DatabaseWrapper(self, fname, tablename)
		self.open_dbs[key] = d
		return d
	def get_scratch_database(self,name,tablename):
		key = (name,tablename)
		if self.scratch_dbs.has_key(key):
			return self.scratch_dbs[key]

		fname = self.scratch_db_fname(name,tablename)
		d = DatabaseWrapper(self, fname, tablename, transact=False)
		self.scratch_dbs[key] = d
		return d
	def remove_database(self,name,tablename):
		key = (name,tablename)
		#
		# Clean up the database handle
		#
		if self.has_key(key):
			self[key].close()
			del self[key]

		#
		# Now actually delete the database file
		#
		fname = self.db_fname(name,tablename)
		d = db.DB(self.dbenv)
		d.remove(fname)
	
	def commit(self):
		self.close_cursors()
		self.txn.commit()
		self.txn = self.dbenv.txn_begin(flags=db.DB_TXN_NOSYNC|db.DB_DIRTY_READ|db.DB_TXN_NOWAIT)
	def abort(self):
		self.close_cursors()
		self.txn.abort()
		self.txn = self.env.txn_begin()
	def db_fname(self,name,tablename):
		return os.path.join(self.global_config.home_area(),name)
	def scratch_db_fname(self,name,tablename):
		return os.path.join(self.global_config.staging_area(),name)
	#
	# Utility methods, not to be called from outside
	#
	def close_cursors(self):
		for (key, d) in self.open_dbs.iteritems():
			d.close_cursor()
		for (key, d) in self.scratch_dbs.iteritems():
			d.close_cursor()
	def close_dbs(self):
		for (key, d) in self.open_dbs.iteritems():
			d.close()
		for (key, d) in self.scratch_dbs.iteritems():
			d.close()
		self.open_dbs = {}
		self.scratch_dbs = {}

class DatabaseWrapper:
	def __init__(self,db_config,filename,dbname,transact=True):
		self.db_config = db_config
		self.filename = filename
		self.dbname = dbname
		self.transact = transact
		
		self.d = db.DB(self.db_config.dbenv)
		self.cursor = None
		
		print "Opening database filename=%s, dbname=%s" %(filename,dbname)
		self.d.open(filename, dbname, db.DB_HASH, db.DB_CREATE, txn=self.get_txn())

	def get_txn(self):
		if self.transact:
			return self.db_config.txn
		return None
	#
	# Access methods
	#
	def get(self,key):
		return self.d.get(str(key), txn=self.get_txn())
	def put(self,key,value):
		self.d.put(str(key),str(value),txn=self.get_txn())
	def __getitem__(self,key):
		return self.get(key)
	def __setitem__(self,key,value):
		return self.put(key,value)
	def __delitem__(self,key):
		self.d.delete(key,txn=self.get_txn())
	def __len__(self):
		stat = self.d.stat()
		return stat['ndata']
	def has_key(self,key):
		return self.get(key) != None
	#
	# Database cleanup options
	#
	def truncate(self):
		self.d.truncate()
	#
	# Transaction support
	#
	def close_cursor(self):
		if self.cursor != None:
			self.cursor.close()
			self.cursor = None
	def close(self):
		self.d.close()
		self.d = None
	#
	# Iteration support
	#
	def __iter__(self):
		self.cursor = self.d.cursor(self.get_txn())
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

