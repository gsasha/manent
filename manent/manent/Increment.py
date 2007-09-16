from cStringIO import StringIO
import time

from IntegerEncodings import *

#TODO: REFACTOR: Remove the followign line after gena's refactoring
#from VersionConfig import VersionConfig

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
	def __init__(self,repository,db,index):
		self.repository = repository
		self.index = index
		self.db = db

		self.readonly = None
		self.finalized = False

	def get_fs_digest(self):
		return self.fs_digest

	def compute_message(self):
		# TODO: Make the message include other data, comment etc.
		m = StringIO()
		
		m.write("index=%d\n" % self.index)
		m.write("time=%s\n" % self.ctime)
		m.write("comment=%s\n" % base64.b64encode(self.comment))
		m.write("fs_digest=%s\n" % base64.b64encode(self.fs_digest))
		if self.finalized:
			m.write("finalized=1")
		else:
			m.write("finalized=0")

		return m.getvalue()

	def parse_message(self,message):
		items = {}
		stream = StringIO(message)
		for line in stream:
			key,value = line.split("=",1)
			items[key]=value

		index = int(items['index'])
		ctime = int(items['time'])
		comment = base64.b64decode(items['comment'])
		fs_digest = base64.b64decode(items['fs_digest'])
		finalized = items['finalized'] == '1'

		return (index,ctime,comment,fs_digest,finalized)
	#
	# Methods for manipulating a newly created increment
	#
	def start(self,index,comment):
		self.index = index
		self.comment = comment
		self.ctime = time.ctime()
		if self.readonly != None:
			raise Exception("Attempt to start an existing increment")
		self.readonly = False

	def finalize(self,fs_digest):
		if self.readonly != False:
			raise Exception("Increment already finalized")
		
		self.fs_digest = fs_digest
		
		print "Finalizing increment", self.fs_digest
		storage_index = self.repository.get_active_storage_index()
		storage_index_str = ascii_encode_int_varlen(storage_index)
		index_str = ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)] = "1"
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = str(self.ctime)
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str
		self.repository.add_block(Digest.dataDigest(message),message,Container.CODE_INCREMENT)
		self.finalized = True
		self.readonly = True

	def dump_intermediate(self,fs_digest):
		if self.readonly != False:
			raise Exception("Increment already finalized")

		self.fs_digest = fs_digest
		
		print "Creating intermediate increment", base64.b64encode(self.fs_digest)
		storage_index = self.repository.get_active_storage_index()
		storage_index_str = ascii_encode_int_varlen(storage_index)
		index_str = ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)] = "0"
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = self.ctime
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str
		message = self.compute_message()
		self.repository.add_block(Digest.dataDigest(message),message,Container.CODE_INCREMENT)

	#
	# Loading an existing increment from db
	#
	def load(self,storage_index,index):
		if self.readonly != None:
			raise "Attempt to load an existing increment"

		storage_index_str = ascii_encode_int_varlen(storage_index)
		index_str = ascii_encode_int_varlen(index)
		self.fs_digest = self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)]
		self.is_finalized = int(self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)]
		self.ctime = int(self.db["Increment.%s.%s.time"%(storage_index_str,index_str)]
		self.comment = self.db["Increment.%s.%s.comment"%(storage_index_str,index_str)]
		assert self.db["Increment.%s.%s.comment"%(storage_index_str,index_str)] == index_str

		self.readonly = True
	#
	# Restoring an increment from backup to db
	#
	def reconstruct(self,digest):
		if self.readonly != None:
			raise "Attempt to restore an existing increment"

		storage_index = self.block_database.get_storage_index(digest)
		message = self.block_database.load_block(digest)
		(self.index,self.ctime,self.comment,self.fs_digest,self.is_finalized) = self.parse_message(message)
		# How do I know the storage index????
		
		if is_finalized:
			self.db["I%d.finalized"%self.index] = "1"
			self.finalized = True

		self.readonly = True
