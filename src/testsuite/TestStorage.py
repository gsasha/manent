#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import shutil
import sys
import tempfile
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
    self.config_db = self.env.get_database_btree("a", None, None)
    self.config_db.truncate()
    self.storage_params = Storage.StorageParams(0, self.env, self.txn,
                                                self.config_db)
    self.scratch_path = tempfile.mkdtemp(".storage", "manent.",
      Config.paths.temp_area())
    self.CONFIGURATION = {"path": self.scratch_path, "encryption_key": "kuku"}
    #print "tmp path=", self.scratch_path
  def tearDown(self):
    shutil.rmtree(self.scratch_path)
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
  def test_sequence_created(self):
    # Test that unique sequence ids are created
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id1 = storage.create_sequence()
    self.storage_params.index += 1
    config_db = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id2 = storage.create_sequence()
    self.failUnless(seq_id1 != seq_id2)
  def test_active_sequence_reloaded(self):
    # Test that the active sequence is reloaded correctly
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.load_configuration(None)
    seq_id2 = storage2.get_active_sequence_id()
    self.assertEqual(seq_id1, seq_id2)
  def test_sequences_reloaded(self):
    # Test that all the sequences created get restored
    # Create storage and a container
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    container = storage1.create_container()
    container.add_block(Digest.dataDigest("stam"), Container.CODE_DATA, "stam")
    container.finish_dump()
    container.upload()
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
    self.assertEquals(
        (seq_id1, 0, Digest.dataDigest("stam"), Container.CODE_DATA),
        handler.sequences[0])
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

    # Reload the storage and read the container
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
    # Create a container in each storage, make sure the containers are mutually
    # visible
    c1 = storage1.create_container()
    c1.add_block(Digest.dataDigest("c1block"), Container.CODE_DATA, "c1block")
    c1.finish_dump()
    c1.upload()
    c2 = storage2.create_container()
    c2.add_block(Digest.dataDigest("c2block"), Container.CODE_DATA, "c2block")
    c2.finish_dump()
    c2.upload()
    # Reload the storages
    class Handler:
      def __init__(self):
        self.containers = []
      def is_requested(self, sequence_id, container_idx, digest, code):
        self.containers.append((sequence_id, container_idx))
        return False
      def loaded(self, digest, code, data):
        pass
    handler1 = Handler()
    storage1.load_sequences(handler1)
    self.assertEqual([(seq_id2, c2.index)], handler1.containers)
    handler2 = Handler()
    storage2.load_sequences(handler2)
    self.assertEqual([(seq_id1, c1.index)], handler2.containers)
  def test_new_containers_in_active_sequence_caught(self):
    # Test that if new containers appear unexpectedly in the active sequence,
    # it is actually discovered.
    logging.debug("----------------1")
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    logging.debug("----------------2")
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.configure(self.CONFIGURATION, None)
    storage2.create_sequence(test_override_sequence_id=seq_id1)
    c = storage2.create_container()
    c.add_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    c.finish_dump()
    c.upload()
    try:
      logging.debug("----------------3")
      storage1.load_sequences(None)
    except:
      pass
    else:
      self.fail("Expected load_sequences to discover the unexpected container")
