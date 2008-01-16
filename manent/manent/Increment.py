import cStringIO as StringIO
import time
import base64

import utils.IntegerEncodings as IE
import manent.utils.Digest as Digest
import Container

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
	def __init__(self, block_database, db):
		self.block_database = block_database
		self.db = db

		self.readonly = None

	# Class method
	def get_increments(cls):
		increments = {}

		increment_rexp = re.compile('Increment\.([^\.]+)\.([^\.]+)')
		for key, value in self.config_db.iteritems_prefix("Increment"):
			if value.endswith("fs_digest"):
				match = increment_rexp.match(key)
				storage_index = IE.ascii_decode_int_varlen(match.group(1))
				index = IE.ascii_decode_int_varlen(match.group(2))

				if not increments.has_key(storage_index):
					increments[storage_index] = []
				increments[storage_index].append(index)
		
		return increments
	get_increments = classmethod(get_increments)

	def index(self):
		return self.index

	def get_fs_digest(self):
		return self.fs_digest

	#
	# Methods for manipulating a newly created increment
	#
	def start(self, storage_index, index, comment):
		if self.readonly is not None:
			raise Exception("Attempt to start an existing increment")
		self.readonly = False
		
		self.index = index
		self.storage_index = storage_index
		self.comment = comment
		self.ctime = int(time.time())
		self.fs_digest = None

	def finalize(self, fs_digest):
		if self.readonly != False:
			raise Exception("Increment already finalized")
		
		self.fs_digest = fs_digest
		self.readonly = True
		
		#print "Finalizing increment", self.fs_digest
		storage_index_str = IE.ascii_encode_int_varlen(self.storage_index)
		index_str = IE.ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = str(self.ctime)
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str
		message = self.__compute_message()
		digest = Digest.dataDigest(message)
		self.block_database.add_block(digest, Container.CODE_INCREMENT_DESCRIPTOR, message)
		return digest

	#
	# Loading an existing increment from db
	#
	def load(self, storage_index, index):
		if self.readonly != None:
			raise "Attempt to load an existing increment"

		self.storage_index = storage_index
		self.index = index
		
		storage_index_str = IE.ascii_encode_int_varlen(storage_index)
		index_str = IE.ascii_encode_int_varlen(index)
		self.fs_digest =     self.db["Increment.%s.%s.fs_digest"%(storage_index_str, index_str)]
		self.ctime     = int(self.db["Increment.%s.%s.time"%(storage_index_str, index_str)])
		self.comment   =     self.db["Increment.%s.%s.comment"%(storage_index_str, index_str)]
		
		assert self.db["Increment.%s.%s.index"%(storage_index_str, index_str)] == index_str

		self.readonly = True

	#
	# Restoring an increment from backup to db
	#
	def reconstruct(self, digest):
		if self.readonly != None:
			raise "Attempt to restore an existing increment"

		#
		# Parse the message from the storage
		#
		storage_index = self.block_database.get_storage_index(digest)
		self.storage_index = storage_index
		message = self.block_database.load_block(digest)
		(self.index, self.ctime, self.comment, self.fs_digest) = self.__parse_message(message)

		#
		# Update the data in the db
		#
		storage_index_str = IE.ascii_encode_int_varlen(storage_index)
		index_str = IE.ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.time"     %(storage_index_str,index_str)] = self.ctime
		self.db["Increment.%s.%s.comment"  %(storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"    %(storage_index_str,index_str)] = index_str

		self.readonly = True
	def __compute_message(self):
		m = StringIO.StringIO()
		m.write("index=%d\n" % self.index)
		m.write("time=%s\n" % self.ctime)
		m.write("comment=%s\n" % base64.b64encode(self.comment))
		m.write("fs_digest=%s\n" % base64.b64encode(self.fs_digest))
		return m.getvalue()

	def __parse_message(self, message):
		items = {}
		stream = StringIO.StringIO(message)
		for line in stream:
			key,value = line.strip().split("=", 1)
			items[key]=value

		index = int(items['index'])
		ctime = int(items['time'])
		comment = base64.b64decode(items['comment'])
		fs_digest = base64.b64decode(items['fs_digest'])

		return (index, ctime, comment, fs_digest)
