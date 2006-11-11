#import hashlib
import md5
import struct
import os, os.path, sys
import re
import bsddb
import base64

from bsddb import db

import Backup
import Container

class DatabaseWrapper:
	def __init__(self,env,filename,dbname,transact):
		self.filename = filename
		self.dbname = dbname
		self.transact = transact
		self.env = env
		self.d = db.DB(self.env)
		self.cursor = None
		if self.transact:
			txn = self.env.txn_begin(flags=db.DB_TXN_NOSYNC|db.DB_DIRTY_READ|db.DB_TXN_NOWAIT)
		else:
			txn = None
		print "Opening database filename=%s, dbname=%s" %(filename,dbname)
		self.d.open(filename, dbname, db.DB_HASH, db.DB_CREATE, txn=txn)
		#self.d.open(filename, dbname, db.DB_BTREE, db.DB_CREATE, txn=txn)
		if self.transact:
			txn.commit()
			self.txn = self.env.txn_begin(flags=db.DB_TXN_NOSYNC|db.DB_DIRTY_READ|db.DB_TXN_NOWAIT)
		else:
			self.txn = None
	def get(self,key):
		return self.d.get(str(key), txn=self.txn)
	def put(self,key,value):
		self.d.put(str(key),str(value),txn=self.txn)
	def __getitem__(self,key):
		return self.get(key)
	def __setitem__(self,key,value):
		return self.put(key,value)
	def __delitem__(self,key):
		self.d.delete(key,txn=self.txn)
	def __len__(self):
		stat = self.d.stat()
		return stat['ndata']
	def has_key(self,key):
		return self.get(key) != None
	def commit(self):
		if not self.transact:
			self.d.sync()
			return
		print "Committing database", self.filename, self.dbname
		if self.cursor != None:
			self.cursor.close()
			self.cursor = None
		self.txn.commit()
		self.env.txn_checkpoint()
		self.txn = self.env.txn_begin()
	def close(self):
		if self.txn:
			# the user should be smart enough to call commit() before close
			# if that's what he wanted!
			self.txn.abort()
			self.txn = None
		self.d.close()
		self.d = None
	def remove(self):
		d = db.DB(self.env)
		print "Removing database", self.filename
		d.remove(self.filename)
		pass
	def abort(self):
		if not self.transact: return
		if self.cursor != None:
			self.cursor = None
		self.txn.abort()
		self.txn = self.env.txn_begin()
	def __iter__(self):
		self.cursor = self.d.cursor(self.txn)
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
	def truncate(self):
		self.d.truncate()

class GlobalConfig:
	def __init__(self):
		self.backups = {}
		self.open_backups = []
		
		if not os.path.isdir(self.homeArea()):
			os.mkdir(self.homeArea())
		dbenv_dir = self.homeArea()+"/dbenv"
		if not os.path.isdir(dbenv_dir):
			os.mkdir(dbenv_dir)
		self.dbenv = db.DBEnv()
		self.dbenv.set_cachesize(0,100*1024*1024)
		self.dbenv.set_lk_max_locks(20000)
		self.dbenv.set_lk_max_objects(20000)
		self.dbenv.set_lk_detect(db.DB_LOCK_DEFAULT)
		self.dbenv.open(dbenv_dir, db.DB_RECOVER|db.DB_CREATE|db.DB_INIT_TXN|db.DB_INIT_MPOOL|db.DB_INIT_LOCK)
		#self.dbenv.open(dbenv_dir, db.DB_CREATE|db.DB_INIT_MPOOL)
		
		#for name in self.dbenv.log_archive():
			#file = os.path.join(self.homeArea(),"dbenv",name)
			#os.unlink(file)
		
	def homeArea(self):
		if os.name == "nt":
			return os.path.join(os.environ["APPDATA"], "manent")
		else:
			return os.path.join(os.environ["HOME"], "manent")

	def close(self):
		for name in self.dbenv.log_archive():
			file = os.path.join(self.homeArea(),"dbenv",name)
			os.unlink(file)
		
		self.dbenv.close()
		dbenv = db.DBEnv()
		result = dbenv.remove(self.homeArea()+"/dbenv")

	def database_exists(self,name,tablename):
		fname = os.path.join(self.homeArea(),name+tablename)
		return os.path.isfile(fname)
	def get_database(self,name,tablename,transact):
		return DatabaseWrapper(self.dbenv, os.path.join(self.homeArea(),name+tablename),tablename,transact)
	def load(self):
		if not os.path.exists(self.homeArea()+"/config"):
			return
		file = open(self.homeArea()+"/config")
		for line in file:
			(label,dataPath,containerType, containerParams) = re.split("\s+",line+" ",3)
			containerParams = re.split("\s+", containerParams.rstrip())
			self.backups[label] = (dataPath, containerType, containerParams)
	def save(self):
		file = open(self.homeArea()+"/config","w")
		for label in self.backups.keys():
			(dataPath,containerType,containerParams) = self.backups[label]
			file.write("%s %s %s %s\n" % (label, dataPath, containerType, " ".join(containerParams)))
	
	def create_backup(self,label,dataPath,containerType,containerParams):
		if label in self.backups:
			raise "Backup %s already exists"%label
		
		backup = Backup.Backup(self,label)
		backup.configure(dataPath,containerType,containerParams)
		self.open_backups.append(backup)
		
		self.backups[label] = (dataPath,containerType,containerParams)
		return backup
	def load_backup(self,label):
		if not label in self.backups:
			raise "Backup %s does not exist"%label
		
		backup = Backup.Backup(self,label)
		(dataPath,containerType,containerParams) = self.backups[label]
		print self.backups[label]
		backup.load(dataPath,containerType,containerParams)
		self.open_backups.append(backup)
		return backup
	def reconstruct_backup(self,label,dataPath,containerType,containerParams):
		if label in self.backups:
			raise "Backup %s already exists" % label
		
		backup = Backup.Backup(self,label)
		backup.reconstruct(dataPath,containerType,containerParams)
		
		self.open_backups.append(backup)
		self.backups[label] = (dataPath,containerType,containerParams)
		return backup
		
	def has_backup(self,label):
		return label in self.backups
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
	# Filesystem config
	#
	def stagingArea(self):
		if os.name == "nt":
			path = os.path.join(os.environ["TEMP"], "manent")
			if (not os.path.exists(path)):
				os.mkdir(path)
			return path
		else:
			return "/tmp"

	def homeArea(self):
		if os.name == "nt":
			return os.path.join(os.environ["APPDATA"], "manent")
		else:
			return os.path.join(os.environ["HOME"], "manent")

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
	