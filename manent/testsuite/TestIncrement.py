import unittest

from manent.Increment import *
from manent.IncrementDatabase import *
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

	def test_start(self):
		"""Test that increment database starts increments correctly"""
		repository = MockRepository()
		blockDB = MockBlockDatabase(repository)
		db = {}

		#
		# Create one increment and see that it produces correct basis
		#
		idb = IncrementDatabase(blockDB,db)
		bases1 = idb.start_increment("test increment 1")
		self.assertEqual(bases1,[])
		
		fs1_digest = Digest.dataDigest("data1")
		idb.finalize_increment(fs1_digest)
		bases2 = idb.start_increment("test increment 2")
		self.assertEqual(bases2,[fs1_digest])
		#
		# See that an intermediate increment produces correct basis
		#
		fs2_digest = Digest.dataDigest("data2")
		idb.dump_intermediate(fs2_digest)
		fs3_digest = Digest.dataDigest("data3")
		idb.dump_intermediate(fs3_digest)
		#
		# Emulate restart of the program: IncrementDB is recreated from
		# the databases
		#
		idb = IncrementDatabase(blockDB,db)
		bases3 = idb.start_increment("test increment 3")
		self.assertEqual(bases3,[fs1_digest,fs3_digest])

		fs4_digest = Digest.dataDigest("data4")
		idb.dump_intermediate(fs4_digest)
		
		idb = IncrementDatabase(blockDB,db)
		bases4 = idb.start_increment("test increment 4")
		self.assertEqual(bases4,[fs1_digest,fs3_digest,fs4_digest])