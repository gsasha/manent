#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import unittest

import manent.Database as Database
import manent.Increment as Increment
import manent.IncrementManager as IncrementManager
import manent.utils.Digest as Digest

import Mock

class TestIncrement(unittest.TestCase):
	def test_load(self):
		"""Test that increment saves to db and loads"""
		repository = Mock.MockRepository()
		blockDB = Mock.MockBlockDatabase(repository)
		db = {}

		increment1 = Increment.Increment(blockDB, db)
		increment2 = Increment.Increment(blockDB, db)
		increment1.start(0,1,"test increment 1")
		increment1.finalize(Digest.dataDigest("aaaaaa"), 0)
		increment2.load(0,1)
		
		self.assertEqual(increment1.comment, increment2.comment)
		self.assertEqual(increment1.fs_digest, increment2.fs_digest)
		self.assertEqual(increment1.ctime, increment2.ctime)
		self.assertEqual(increment1.index, increment2.index)
		self.assertEqual(increment1.storage_index, increment2.storage_index)

	def test_reconstruct(self):
		"""Test that increment can reconstruct itself"""
		repository = Mock.MockRepository()
		blockDB = Mock.MockBlockDatabase(repository)
		db = {}

		# create the increment
		increment1 = Increment.Increment(blockDB, db)
		increment1.start(0,1,"test increment 1")
		digest1 = increment1.finalize(Digest.dataDigest("aaaaa"), 1)
		
		# Reconstruct the increment from the digest
		increment2 = Increment.Increment(blockDB, db)
		increment2.reconstruct(digest1)
		self.assertEqual(increment1.comment, increment2.comment)
		self.assertEqual(increment1.fs_digest, increment2.fs_digest)
		self.assertEqual(increment1.ctime, increment2.ctime)
		self.assertEqual(increment1.index, increment2.index)
		self.assertEqual(increment1.storage_index, increment2.storage_index)

	def test_start(self):
		"""Test that increment database starts increments correctly"""
		env = Database.PrivateDatabaseManager()
		txn = Database.TransactionHandler(env)
		#
		# Create one increment and see that it produces correct basis
		#
		class MockStorageManager:
			def __init__(self):
				self.blocks = {}
			def get_active_storage_index(self):
				return 0
			def add_block(self, digest, code, data):
				self.blocks[digest] = (code, data)
		msm = MockStorageManager()
		idb = IncrementManager.IncrementManager(env, txn, msm)
		bases1, level1 = idb.start_increment("test increment 1")
		self.assertEqual(bases1, None)
		self.assertEqual(level1, None)

		fs1_digest = Digest.dataDigest("data1")
		fs1_level = 0
		idb.finalize_increment(fs1_digest, fs1_level)
		bases2, level2 = idb.start_increment("test increment 2")
		# Unfinalized increment is not returned
		self.assertEqual(bases2, fs1_digest)
		self.assertEqual(level2, fs1_level)
		#
		# Emulate restart of the program: IncrementDB is recreated from
		# the databases
		#
		idb = IncrementManager.IncrementManager(env, txn, msm)
		bases3, level3 = idb.start_increment("test increment 3")
		self.assertEqual(bases3, fs1_digest)
		self.assertEqual(level3, fs1_level)
		
		idb = IncrementManager.IncrementManager(env, txn, msm)
		bases4, level4 = idb.start_increment("test increment 4")
		self.assertEqual(bases4, fs1_digest)
		self.assertEqual(level4, fs1_level)
