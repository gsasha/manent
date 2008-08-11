#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import os.path
import shutil
import stat
import sys
import tempfile
import traceback
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.utils.Digest as Digest

class TestStorage(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.scratch_path = tempfile.mkdtemp(u".storage", "manent.",
      Config.paths.temp_area())
    self.CONFIGURATION = {"path": self.scratch_path, "encryption_key": "kuku"}
    self.config_db = self.env.get_database_btree("a", None, None)
    self.storage_params = Storage.StorageParams(0, self.env, self.txn,
                                                self.config_db)
    #print "tmp path=", self.scratch_path
  def init_config_db(self):
    config_db = self.env.get_database_btree("a", None, None)
    self.storage_params.config_db = config_db
  def tearDown(self):
    self.txn = None
    self.env.close()
    self.env = None
    def set_writable(arg, dirname, names):
      # Set the files back to writable, otherwise, rmtree can't remove them on
      # native Windows.
      for name in names:
        # on windows, for which this is written, there's no os.S_IWRITE :(
        os.chmod(os.path.join(dirname, name), stat.S_IWRITE)
    if os.name == 'nt':
      os.path.walk(self.scratch_path, set_writable, None)
    shutil.rmtree(self.scratch_path)
    Config.paths.clean_temp_area()
  def test_params_stored(self):
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    # Make sure that the configuration we put in is read back correctly
    self.assertEqual(storage.get_config(), self.CONFIGURATION)
    
    # Recreate the storage from config_db and make sure its configuration
    # returns back.
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.get_config()
    self.assertEqual(storage.get_config(), self.CONFIGURATION)
    storage.close()
  def test_container_name(self):
    storage = Storage.Storage(self.storage_params)
    for container_idx in range(10000):
      seq_id = os.urandom(12)
      encoded = Storage.encode_container_name(seq_id, container_idx, "data")
      #print "encoded", encoded
      dec_seq_id, dec_container_idx, dec_ext = Storage.decode_container_name(encoded)
      self.assertEqual(seq_id, dec_seq_id)
      self.assertEqual(container_idx, dec_container_idx)
      self.assertEqual(dec_ext, "data")
    storage.close()
  def test_sequence_created(self):
    # Test that unique sequence ids are created
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id1 = storage.create_sequence()
    self.storage_params.index += 1
    storage.close()
    config_db = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id2 = storage.create_sequence()
    self.failUnless(seq_id1 != seq_id2)
    storage.close()
  def test_active_sequence_reloaded(self):
    # Test that the active sequence is reloaded correctly
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    storage1.close()
    self.init_config_db()
    
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.load_configuration(None)
    seq_id2 = storage2.get_active_sequence_id()
    self.assertEqual(seq_id1, seq_id2)
    storage2.close()
  def test_sequences_reloaded(self):
    # Test that all the sequences created get restored
    # Create storage and a container
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    for i in range(4):
      container = storage1.create_container()
      container.add_block(Digest.dataDigest("m"), Container.CODE_DATA, "m")
      container.finish_dump()
      container.upload()
    storage1.close()
    # Create a new db to simulate a different machine
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    class Handler:
      def __init__(self):
        self.sequences = []
      def is_requested(self, sequence_id, container_idx, digest, code):
        self.sequences.append((sequence_id, container_idx, digest, code))
        return False
    handler = Handler()
    storage2.configure(self.CONFIGURATION, handler)
    self.assert_((seq_id1, 0, Digest.dataDigest("m"), Container.CODE_DATA) in
        handler.sequences)
    storage2.close()
  def test_container_created(self):
    # Test that containers are created and restored correctly.
    # Create storage and a container
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id = storage.create_sequence()
    container = storage.create_container()
    block = "some strange text"
    block_digest = Digest.dataDigest(block)
    container.add_block(block_digest, Container.CODE_DATA, block)
    container.finish_dump()
    container.upload()
    self.assertEqual(0, container.get_index())
    self.assertEqual(seq_id, container.get_sequence_id())
    storage.close()

    # Reload the storage and read the container
    self.init_config_db()
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.load_configuration(None)
    container = storage.get_container(seq_id, 0)
    class Handler:
      def __init__(self):
        self.blocks = []
      def is_requested(self, digest, code):
        return True
      def loaded(self, digest, code, data):
        self.blocks.append((digest, code, data))
    handler = Handler()
    container.load_blocks(handler)
    logging.debug("Blocks: " + str(handler.blocks))
    data_blocks = [b for b in handler.blocks if b[1] == Container.CODE_DATA]
    self.assertEqual(block_digest, data_blocks[0][0])
    storage.close()
  def test_new_containers_visible(self):
    # Test that the new containers appearing in all the sequences are visible
    # Create two storages at the same place
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.configure(self.CONFIGURATION, None)
    seq_id2 = storage2.create_sequence()
    self.assert_(seq_id1 != seq_id2)
    # Create 4 containers in each storage, make sure the containers are mutually
    # visible
    c1s = []
    c2s = []
    for i in range(4):
      c1 = storage1.create_container()
      c1.add_block(Digest.dataDigest("c1block%d" % i),
          Container.CODE_DATA, "c1block%d" % i)
      c1.finish_dump()
      c1.upload()
      c1s.append((seq_id1, c1.index))
      c2 = storage2.create_container()
      c2.add_block(Digest.dataDigest("c2block%d" % i),
          Container.CODE_DATA, "c2block%d" % i)
      c2.finish_dump()
      c2.upload()
      c2s.append((seq_id2, c2.index))
    # Reload the storages
    class Handler:
      def __init__(self):
        self.containers = []
      def is_requested(self, sequence_id, container_idx, digest, code):
        if not (sequence_id, container_idx) in self.containers: 
          # Since the last container is a summary, it will ask for all the
          # piggybacking containers too. Must make sure we register every new
          # container exactly once.
          self.containers.append((sequence_id, container_idx))
        return False
      def loaded(self, digest, code, data):
        pass
    handler1 = Handler()
    storage1.load_sequences(handler1)
    self.assertEqual(c2s, sorted(handler1.containers))
    handler2 = Handler()
    storage2.load_sequences(handler2)
    self.assertEqual(c1s, sorted(handler2.containers))
    storage1.close()
    storage2.close()
  def test_new_containers_in_active_sequence_caught(self):
    # Test that if new containers appear unexpectedly in the active sequence,
    # it is actually discovered.
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.configure(self.CONFIGURATION, None)
    storage2.create_sequence(test_override_sequence_id=seq_id1)
    # We need to create 4 countainers, since the first 3 are non-summary and
    # will thus not be discovered.
    for i in range(4):
      c = storage2.create_container()
      c.add_block(Digest.dataDigest("aaa%d" % i),
                  Container.CODE_DATA, "aaa%d" % i)
      c.finish_dump()
      c.upload()
    try:
      storage1.load_sequences(None)
    except:
      traceback.print_exc()
      pass
    else:
      self.fail("Expected load_sequences to discover the unexpected container")
    try:
      storage1.close()
      storage2.close()
    except:
      # Ok, we can't. just pass.
      pass

suite = unittest.TestLoader().loadTestsFromTestCase(TestStorage)
if __name__ == "__main__":
  unittest.TextTestRunner(verbosity=2).run(suite)

