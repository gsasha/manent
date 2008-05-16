#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import cStringIO as StringIO
import time

import Container
import utils.Digest as Digest
import utils.IntegerEncodings as IE

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
	def __init__(self, block_database, db):
		self.block_database = block_database
		self.db = db

		self.readonly = None

	def index(self):
		return self.index

	def get_fs_digest(self):
		return self.fs_digest
	def get_fs_level(self):
		return self.fs_level

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
		self.fs_level = None

	def finalize(self, fs_digest, fs_level):
		if self.readonly != False:
			raise Exception("Increment already finalized")
		
		self.fs_digest = fs_digest
		self.fs_level = fs_level
		self.readonly = True
		
		#print "Finalizing increment", self.fs_digest
		storage_index_str = IE.ascii_encode_int_varlen(self.storage_index)
		index_str = IE.ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest" %
        (storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.fs_level"  %
        (storage_index_str,index_str)] = str(self.fs_level)
		self.db["Increment.%s.%s.time"      %
        (storage_index_str,index_str)] = str(self.ctime)
		self.db["Increment.%s.%s.comment"   %
        (storage_index_str,index_str)] = self.comment
		self.db["Increment.%s.%s.index"     %
        (storage_index_str,index_str)] = index_str
		message = self.__compute_message()
		digest = Digest.dataDigest(message)
		self.block_database.add_block(
        digest, Container.CODE_INCREMENT_DESCRIPTOR, message)
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
		self.fs_digest =     self.db["Increment.%s.%s.fs_digest" %
        (storage_index_str, index_str)]
		self.fs_level  = int(self.db["Increment.%s.%s.fs_level" %
      (storage_index_str, index_str)])
		self.ctime     = int(self.db["Increment.%s.%s.time" %
      (storage_index_str, index_str)])
		self.comment   =     self.db["Increment.%s.%s.comment" %
        (storage_index_str, index_str)]
		
		assert self.db["Increment.%s.%s.index" %
        (storage_index_str, index_str)] == index_str

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
		(self.index, self.ctime, self.comment, self.fs_digest, self.fs_level) =\
			self.__parse_message(message)

		#
		# Update the data in the db
		#
		storage_index_str = IE.ascii_encode_int_varlen(storage_index)
		index_str = IE.ascii_encode_int_varlen(self.index)
		self.db["Increment.%s.%s.fs_digest"%(storage_index_str,index_str)] = self.fs_digest
		self.db["Increment.%s.%s.fs_level" %(storage_index_str,index_str)] = self.fs_level
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
		m.write("fs_level=%s\n" % str(self.fs_level))
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
		fs_level = int(items['fs_level'])

		return (index, ctime, comment, fs_digest, fs_level)
