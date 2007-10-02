import unittest

from manent.Increment import *
import manent.utils.Digest as Digest

from Mock import *

class TestIncrement(unittest.TestCase):
	def test_load(self):
		"""Test that increment saves to db and loads"""
		repository = MockRepository()
		blockDB = MockBlockDatabase(repository)
		db = {}

		increment1 = Increment(blockDB,db)
		increment2 = Increment(blockDB,db)
		increment1.start(0,1,"test increment 1")
		increment1.finalize(Digest.dataDigest("aaaaaa"))
		increment2.load(0,1)
		
		self.assertEqual(increment1.comment, increment2.comment)
		self.assertEqual(increment1.fs_digest, increment2.fs_digest)
		self.assertEqual(increment1.ctime, increment2.ctime)
		self.assertEqual(increment1.finalized, increment2.finalized)
		self.assertEqual(increment1.index, increment2.index)
		self.assertEqual(increment1.storage_index, increment2.storage_index)
		self.assertEqual(increment1.erases_prev, increment2.erases_prev)

	def test_reconstruct(self):
		"""Test that increment can reconstruct itself"""
		repository = MockRepository()
		blockDB = MockBlockDatabase(repository)
		db = {}

		# create the increment
		increment1 = Increment(blockDB,db)
		increment1.start(0,1,"test increment 1")
		digest1 = increment1.finalize(Digest.dataDigest("aaaaa"))
		
		# Reconstruct the increment from the digest
		increment2 = Increment(blockDB,db)
		increment2.reconstruct(digest1)
		self.assertEqual(increment1.comment, increment2.comment)
		self.assertEqual(increment1.fs_digest, increment2.fs_digest)
		self.assertEqual(increment1.ctime, increment2.ctime)
		self.assertEqual(increment1.finalized, increment2.finalized)
		self.assertEqual(increment1.index, increment2.index)
		self.assertEqual(increment1.storage_index, increment2.storage_index)
		self.assertEqual(increment1.erases_prev, increment2.erases_prev)
		