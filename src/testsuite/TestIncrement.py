#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import sys
import traceback
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Database as Database
import manent.Increment as Increment
import manent.IncrementManager as IncrementManager
import manent.Nodes as Nodes
import manent.utils.Digest as Digest

import Mock

class TestIncrement(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
  def tearDown(self):
    self.txn.abort()
    self.txn = None
    self.env.close()
    self.env = None
    Config.paths.clean_temp_area()

  def test_load(self):
    # Test that increment saves to db and loads
    repository = Mock.MockRepository()
    blockDB = Mock.MockBlockDatabase(repository)
    db = self.env.get_database_btree("a", None, None)

    increment1 = Increment.Increment(blockDB, db)
    increment2 = Increment.Increment(blockDB, db)
    increment1.start(0, 1, "backup1", "test increment 1")
    increment1.finalize(Digest.dataDigest("aaaaaa"), 0, Nodes.NULL_STAT, 10)
    increment2.load(0, 1)
    
    for attr in ["comment", "fs_digest", "ctime", "ftime", "index",
        "storage_index"]:
      self.assertEqual(increment1.get_attribute(attr),
          increment2.get_attribute(attr))

  def test_reconstruct(self):
    # Test that increment can reconstruct itself
    # TODO(gsasha): make reconstruct accept seq id and test it.
    repository = Mock.MockRepository()
    blockDB = Mock.MockBlockDatabase(repository)
    db = self.env.get_database_btree("a", None, None)

    # create the increment
    increment1 = Increment.Increment(blockDB, db)
    increment1.start(0, 1, "backup1", "test increment 1")
    digest1 = increment1.finalize(Digest.dataDigest("aaaaa"),
                                  1, Nodes.NULL_STAT, 1)
    data1 = increment1.compute_message()
    
    # Reconstruct the increment from the digest
    increment2 = Increment.Increment(blockDB, db)
    increment2.reconstruct("seq_id1", data1)

    for attr in ["comment", "fs_digest", "ctime", "ftime", "index",
        "storage_index"]:
      self.assertEqual(increment1.get_attribute(attr),
          increment2.get_attribute(attr))

  def test_start(self):
    # Test that increment database starts increments correctly
    #
    # Create one increment and see that it produces correct basis
    #
    class MockStorageManager:
      def __init__(self):
        self.blocks = {}
        self.listeners = []
      def get_active_storage_index(self):
        return 0
      def add_block(self, digest, code, data):
        self.blocks[digest] = (code, data)
      def add_block_listener(self, listener):
        self.listeners.append(listener)
    msm = MockStorageManager()
    idb = IncrementManager.IncrementManager(self.env, self.txn, "backup1", msm)

    bases1, level1, num_f1 = idb.start_increment("test increment 1")
    self.assertEqual(bases1, None)
    self.assertEqual(level1, None)

    fs1_digest = Digest.dataDigest("data1")
    fs1_level = 0
    idb.finalize_increment(fs1_digest, fs1_level, Nodes.NULL_STAT, 1)
    bases2, level2, num_files2 = idb.start_increment("test increment 2")
    # Unfinalized increment is not returned
    self.assertEqual(bases2, fs1_digest)
    self.assertEqual(level2, fs1_level)
    idb.close()
    #
    # Emulate restart of the program: IncrementDB is recreated from
    # the databases
    #
    idb = IncrementManager.IncrementManager(self.env, self.txn, "backup1", msm)
    bases3, level3, num_f3 = idb.start_increment("test increment 3")
    self.assertEqual(bases3, fs1_digest)
    self.assertEqual(level3, fs1_level)
    idb.close()
    
    idb = IncrementManager.IncrementManager(self.env, self.txn, "backup1", msm)
    bases4, level4, num_f4 = idb.start_increment("test increment 4")
    self.assertEqual(bases4, fs1_digest)
    self.assertEqual(level4, fs1_level)
    idb.close()

suite = unittest.TestLoader().loadTestsFromTestCase(TestIncrement)
if __name__ == "__main__":
  unittest.TextTestRunner(verbosity=2).run(suite)
    
