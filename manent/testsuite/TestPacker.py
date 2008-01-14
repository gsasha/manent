import unittest
import random
import base64

import manent.PackerStream as PackerStream
import manent.Container as Container

class MockBackup:
	def __init__(self):
		self.blocks_db = {}
		self.block_code_db = {}
	def get_block_size(self):
		# Use a relatively small block size to test going deep into hierarchy
		return 256
	def add_block(self, digest, code, data):
		#print "Adding block digest=", base64.b64encode(digest), "code=",code
		self.blocks_db[digest] = data
		self.block_code_db[digest] = code
	def load_block(self, digest):
		#print "Loading block digest=", base64.b64encode(digest)
		return self.blocks_db[digest]
	def get_block_code(self, digest):
		return self.block_code_db[digest]

class TestPacker(unittest.TestCase):
	def test_empty_file(self):
		"""Test the packing and unpacking a "nothing sent" file"""
		backup = MockBackup()
		ostream = PackerStream.PackerOStream(backup, Container.CODE_DATA)
		digest = ostream.get_digest()

		istream = PackerStream.PackerIStream(backup, digest)
		data = istream.read()
		self.assertEqual(data, "")
	def test_short_file(self):
		"""Test the packing and unpacking a small file"""
		backup = MockBackup()
		for size in range(1024):
			ostream = PackerStream.PackerOStream(backup, Container.CODE_DATA)
			ostream.write('a' * size)
			digest = ostream.get_digest()

			istream = PackerStream.PackerIStream(backup, digest)
			self.assertEqual(istream.read(), 'a' * size)

	def test_large_file(self):
		"""Test the packing and unpacking of a large file"""
		backup = MockBackup()
		size = 1
		for bsize in range(10):
			ostream = PackerStream.PackerOStream(backup, Container.CODE_DATA)
			for i in range(size):
				ostream.write('a' * 1024)
			digest = ostream.get_digest()

			istream = PackerStream.PackerIStream(backup, digest)
			for i in range(size):
				self.assertEqual(istream.read(1024), 'a'*1024)
			self.assertEqual(istream.read(1), '')

			size *= 2
	def test_different_code(self):
		"""Test that codes different from CODE_DATA work too"""
		backup = MockBackup()
		size = 1
		for bsize in range(10):
			ostream = PackerStream.PackerOStream(backup, Container.CODE_DIR)
			for i in range(size):
				ostream.write('a' * 1024)
			digest = ostream.get_digest()

			istream = PackerStream.PackerIStream(backup, digest)
			for i in range(size):
				self.assertEqual(istream.read(1024), 'a' * 1024)
			self.assertEqual(istream.read(1), '')

			size *= 2
		