import unittest
import random
from cStringIO import StringIO
import os

import manent.utils.Digest
from manent.Container import *

random.seed(23423)

class MockHandler:
	def __init__(self):
		self.expected = {}
		self.data_loaded = {}
	def add_expected(self,digest,data,code):
		self.expected[digest] = (data,code)
	def is_requested(self,digest,code):
		if code >= CODE_CONTROL_START:
			return False
		if self.expected.has_key(digest):
			return True
		if code != CODE_DATA:
			return True
		return False
	def loaded(self,digest,data,code):
		self.data_loaded[digest] = (data,code)
	def check(self):
		for k in self.data_loaded.keys():
			if not self.expected.has_key(k):
				print "block %s not expected" % base64.b64encode(k)
				return False
			assert self.expected.has_key(k)
			assert self.data_loaded.has_key(k)
			expected = self.expected[k][0]
			loaded = self.data_loaded[k][0]
			if expected != loaded:
				print "block %s has loaded different data" % base64.b64encode(k)
				print " Expected %d:"%len(expected), base64.b64encode(expected)
				print " Loaded   %d:"%len(loaded), base64.b64encode(loaded)
				return False
		for k in self.expected.keys():
			if not self.data_loaded.has_key(k):
				print "block %s:%d not loaded" % (base64.b64encode(k),self.expected[k][1])
				return False
		return True

class MockStorage:
	def __init__(self,password):
		self.password = password
		self.header_file_name = "/tmp/manent.test_container.header"
		self.body_file_name = "/tmp/manent.test_container.body"
		self.cur_index = 0
		
	def get_container(self,index):
		container = Container(self,self.header_file_name,self.body_file_name)
		container.start_load(index)
		return container

	def get_password(self):
		return self.password
	def load_container_header(self,index):
		pass
	def load_container_body(self,index):
		pass
	def get_label(self):
		return "mukakaka"
	
	def create_container(self):
		container = Container(self,self.header_file_name,self.body_file_name)
		container.start_dump()
		return container

	def finalize_container(self,container):
		container.finish_dump(self.cur_index)
		self.cur_index += 1

DATA = [
	"",
	"asdf;kasdfj;dlfksdjf;lfjdsfl;dsfdjsfsdf",
	"affdfdfdffasffdffdf",
	"manent is a backup program that supports",
	"encryption, compression and bandwitdh conservation"
	"manent is a backup program that supports",
	"encryption, compression and bandwitdh conservation"
	"manent is a backup program that supports",
	"encryption, compression and bandwitdh conservation"
	]

class TestContainer(unittest.TestCase):

	def test_data_dumper(self):
		"""Basic test of data dumper: data in, data out"""
		handler = MockHandler()
		outfile = StringIO()
		dumper = DataDumper(outfile)

		for d in DATA:
			digest = Digest.dataDigest(d)
			dumper.add_block(digest,d,CODE_DATA)
			handler.add_expected(digest,d,CODE_DATA)

		infile = StringIO(outfile.getvalue())
		blocks = dumper.get_blocks()
		
		undumper = DataDumpLoader(infile,blocks,password=None)
		undumper.load_blocks(handler)

		self.failUnless(handler.check())
	
	def test_data_dumper_compress(self):
		"""Test data dumper when compression is enabled"""
		handler = MockHandler()
		outfile = StringIO()
		dumper = DataDumper(outfile)
		
		dumper.start_compression(CODE_COMPRESSION_BZ2)
		for d in DATA:
			digest = Digest.dataDigest(d)
			dumper.add_block(digest,d,CODE_DATA)
			handler.add_expected(digest,d,CODE_DATA)
		dumper.stop_compression()

		infile = StringIO(outfile.getvalue())
		blocks = dumper.get_blocks()
		
		undumper = DataDumpLoader(infile,blocks,password=None)
		undumper.load_blocks(handler)

		self.failUnless(handler.check())

	def test_data_dumper_encrypt(self):
		"""Test data dumper when encryption is enabled"""
		handler = MockHandler()
		outfile = StringIO()
		dumper = DataDumper(outfile)

		seed = Digest.dataDigest("1")
		dumper.start_encryption(CODE_ENCRYPTION_ARC4,seed,"kakamaika")
		for d in DATA:
			digest = Digest.dataDigest(d)
			dumper.add_block(digest,d,CODE_DATA)
			handler.add_expected(digest,d,CODE_DATA)
		dumper.stop_encryption()

		infile = StringIO(outfile.getvalue())
		blocks = dumper.get_blocks()

		undumper = DataDumpLoader(infile,blocks,password="kakamaika")
		undumper.load_blocks(handler)

		self.failUnless(handler.check())

	def test_data_dumper_stress(self):
		"""Test with really lots of randomly generated data"""
		handler = MockHandler()
		outfile = StringIO()
		dumper = DataDumper(outfile)

		encryption_active = None
		compression_active = None

		known_blocks = {}

		for i in range(10000):
			action = random.randint(0,2)
			
			if compression_active is not None:
				compression_active -= 1
				if compression_active == 0:
					dumper.stop_compression()
					compression_active = None
				else:
					#print "  Compression has %d rounds to go"%compression_active
					pass

			if encryption_active is not None:
				encryption_active -= 1
				if encryption_active == 0:
					if compression_active is not None:
						# If we need to stop encryption, compression must be stopped first
						dumper.stop_compression()
					#print "  Stopping encryption"
					dumper.stop_encryption()
					encryption_active = None
				else:
					#print "  Encryption has %d rounds to go"%encryption_active
					pass

			if action==0:
				# Generate new data item
				data_size = random.randint(0,1000)
				data = os.urandom(data_size)
				code = random.choice([CODE_DATA, CODE_DIR, CODE_DATA, CODE_DATA_PACKER, CODE_DATA, CODE_DIR_PACKER, CODE_DATA, CODE_INCREMENT_START, CODE_INCREMENT_END])
				digest = Digest.dataDigest(data)

				if code == CODE_DATA and known_blocks.has_key(digest):
					# We can't expect the same data block to be added twice to a container
					continue
				known_blocks[digest] = 1
				
				dumper.add_block(digest,data,code)

				# test not requesting to reload every CODE_DATA item
				if code != CODE_DATA or random.randint(0,100)>90:
					handler.add_expected(digest,data,code)

			elif action==1:
				#continue
				# Try to start encryption
				# We can start encryption only if it is not active already
				if encryption_active != None:
					continue
				if compression_active != None:
					continue
				encryption_active = random.randint(1,100)
				#print "  Starting encryption for %d rounds"
				seed = os.urandom(Digest.dataDigestSize())
				dumper.start_encryption(CODE_ENCRYPTION_ARC4,seed,"kakamaika")
				
			elif action==2:
				# Try to start compression
				if compression_active != None:
					continue
				compression_active = random.randint(1,100)
				if encryption_active != None:
					compression_active = min(compression_active,encryption_active)
				algorithm = random.choice([CODE_COMPRESSION_BZ2])
				dumper.start_compression(algorithm)

		if compression_active is not None:
			dumper.stop_compression()
		if encryption_active is not None:
			dumper.stop_encryption()

		infile = StringIO(outfile.getvalue())
		blocks = dumper.get_blocks()
		#print "blocks:"
		#for digest,size,code in blocks:
			#print base64.b64encode(digest), size, code

		undumper = DataDumpLoader(infile,blocks,password="kakamaika")
		undumper.load_blocks(handler)

		self.failUnless(handler.check())

	def test_container(self):
		storage = MockStorage(password="kakamaika")
		handler = MockHandler()

		container = storage.create_container()
		for d in DATA:
			container.add_block(Digest.dataDigest(d),d,CODE_DATA)
			handler.add_expected(Digest.dataDigest(d),d,CODE_DATA)
		storage.finalize_container(container)
		index = container.index

		container = storage.get_container(index)
		container.load_header()
		container.load_body()

		container.load_blocks(handler)
		self.failUnless(handler.check())
