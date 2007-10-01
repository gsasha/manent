from cStringIO import StringIO
import time
import base64

from utils.IntegerEncodings import *
import manent.utils.Digest as Digest
import Container

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
	def __init__(self,repository,db):
		self.repository = repository
		self.db = db

		self.readonly = None
		self.finalized = False

	def index(self):
		return self.index

	def get_fs_digest(self):
		return self.fs_digest

	def compute_message(self):
		m = StringIO()
		m.write("index=%d\n" % self.index)
		m.write("time=%s\n" % self.ctime)
		m.write("comment=%s\n" % base64.b64encode(self.comment))
		m.write("fs_digest=%s\n" % base64.b64encode(self.fs_digest))
		if self.finalized:
			m.write("finalized=1\n")
		else:
			m.write("finalized=0\n")
		if self.erases_prev is not None:
			m.write("erases_prev=%d" % str(self.erases_prev))

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
		if items.has_key('erases_prev'):
			erases_prev = int(items['erases_prev'])

		return (index,ctime,comment,fs_digest,finalized,erases_prev)
	#
	# Methods for manipulating a newly created increment
	#
	def start(self,storage_index,index,comment):
		if self.readonly is not None:
			raise Exception("Attempt to start an existing increment")
		self.readonly = False
		
		self.index = index
		self.storage_index = storage_index
		self.comment = comment
		self.ctime = int(time.time())
		self.fs_digest = None
		self.erases_prev = None

	def finalize(self,fs_digest):
		if self.readonly != False:
			raise Exception("Increment already finalized")
		
		self.fs_digest = fs_digest
		
		#print "Finalizing increment", self.fs_digest
		storage_index_str = ascii_encode_int_varlen(self.storage_index)
		index_str = ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)] = "1"
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = str(self.ctime)
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str
		message = self.compute_message()
		self.repository.add_block(Digest.dataDigest(message),message,Container.CODE_INCREMENT_DESCRIPTOR)
		self.finalized = True
		self.readonly = True

	def dump_intermediate(self,fs_digest):
		if self.readonly != False:
			raise Exception("Increment already finalized")

		self.fs_digest = fs_digest
		
		#print "Creating intermediate increment", base64.b64encode(self.fs_digest)
		storage_index_str = ascii_encode_int_varlen(self.storage_index)
		index_str = ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)] = "0"
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = self.ctime
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str
		message = self.compute_message()
		self.repository.add_block(Digest.dataDigest(message),message,Container.CODE_INCREMENT_DESCRIPTOR)

	#
	# Loading an existing increment from db
	#
	def load(self,storage_index,index):
		if self.readonly != None:
			raise "Attempt to load an existing increment"

		self.storage_index = storage_index
		self.index = index
		
		storage_index_str = ascii_encode_int_varlen(storage_index)
		index_str = ascii_encode_int_varlen(index)
		self.fs_digest =     self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)]
		self.finalized = int(self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)])
		self.ctime     = int(self.db["Increment.%s.%s.time"%(storage_index_str,index_str)])
		self.comment   =     self.db["Increment.%s.%s.comment"%(storage_index_str,index_str)]
		assert self.db["Increment.%s.%s.index"%(storage_index_str,index_str)] == index_str

		self.readonly = True

	#
	# Erasing a previous increment from db
	#
	def erase_previous(self,previous_index):
		if self.readonly != None:
			raise "Attempt to call erase_previous on an existing increment"

		assert self.index == previous_index+1

		prev_storage_index_str = ascii_encode_int_varlen(self.storage_index)
		prev_index_str = ascii_encode_int_varlen(previous_index)

		assert self.db.has_key("Increment.%s.%s.fs_digest"%(prev_storage_index_str,prev_index_str))
		del self.db["Increment.%s.%s.fs_digest"%(prev_storage_index_str,prev_index_str)]
		del self.db["Increment.%s.%s.finalized"%(prev_storage_index_str,prev_index_str)]
		del self.db["Increment.%s.%s.time"%(prev_storage_index_str,prev_index_str)]
		del self.db["Increment.%s.%s.comment"%(prev_storage_index_str,prev_index_str)]
		del self.db["Increment.%s.%s.index"%(prev_storage_index_str,prev_index_str)]

		self.erases_prev = previous_index

	#
	# Restoring an increment from backup to db
	#
	def reconstruct(self,digest):
		if self.readonly != None:
			raise "Attempt to restore an existing increment"

		#
		# Parse the message from the storage
		#
		storage_index = self.block_database.get_storage_index(digest)
		message = self.block_database.load_block(digest)
		(self.index,self.ctime,self.comment,self.fs_digest,self.is_finalized,erases_prev) = self.parse_message(message)

		#
		# Update the data in the db
		#
		storage_index_str = ascii_encode_int_varlen(self.storage_index)
		index_str = ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		if self.is_finalized:
			finalized_str = '1'
		else:
			finalized_str = '0'
		self.db["Increment.%s.%s.finalized"%(storage_index_str,index_str)] = finalized_str
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = self.ctime
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str

		#
		# See if we need to erase the prev increment
		#
		if erases_prev:
			self.erase_previous(erases_prev)

		self.readonly = True
