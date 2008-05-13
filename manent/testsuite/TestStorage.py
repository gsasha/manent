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
    """Test that unique sequence ids are created"""
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id1 = storage.create_sequence()
    self.storage_params.index += 1
    storage = Storage.DirectoryStorage(self.storage_params)
    storage.configure(self.CONFIGURATION, None)
    seq_id2 = storage.create_sequence()
    self.failUnless(seq_id1 != seq_id2)
  def test_active_sequence_reloaded(self):
    """Test that the active sequence is reloaded correctly"""
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.load_configuration(None)
    seq_id2 = storage2.get_active_sequence_id()
    self.assertEqual(seq_id1, seq_id2)
  def test_sequences_reloaded(self):
    """Test that all the sequences created get restored"""
    # Create storage and a container
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    container = storage1.create_container()
    container.finish_dump()
    container.upload()
    seq_id2 = storage1.create_sequence()
    container = storage1.create_container()
    container.finish_dump()
    container.upload()
    # Create a new db to simulate a different machine
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    class NopHandler:
      def __init__(self):
        pass
      def report_new_container(self, container):
        pass
    # We know that there are new containers appearing.
    # However, we don't care in this test.
    storage2.configure(self.CONFIGURATION, NopHandler())
    sequences = storage2.get_sequence_ids()
    #print sequences
    self.assert_(seq_id1 in sequences)
    self.assert_(seq_id2 in sequences)
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
    c1.finish_dump()
    c1.upload()
    c2 = storage2.create_container()
    c2.finish_dump()
    c2.upload()
    # Reload the storages
    class Handler:
      def __init__(self):
        self.containers = []
      def report_new_container(self, container):
        self.containers.append((container.get_sequence_id(),
                    container.get_index()))
      def get_containers(self):
        return self.containers
    handler1 = Handler()
    storage1.load_sequences(handler1)
    self.assertEqual([(seq_id2, c2.index)], handler1.get_containers())
    handler2 = Handler()
    storage2.load_sequences(handler2)
    self.assertEqual([(seq_id1, c1.index)], handler2.get_containers())
  def test_new_containers_in_active_sequence_caught(self):
    """Test that if new containers appear unexpectedly in the active sequence,
    it is actually discovered"""
    storage1 = Storage.DirectoryStorage(self.storage_params)
    storage1.configure(self.CONFIGURATION, None)
    seq_id1 = storage1.create_sequence()
    config_db2 = self.env.get_database_btree("b", None, None)
    self.storage_params.config_db = config_db2
    storage2 = Storage.DirectoryStorage(self.storage_params)
    storage2.configure(self.CONFIGURATION, None)
    storage2.create_sequence(test_override_sequence_id=seq_id1)
    c = storage2.create_container()
    c.finish_dump()
    c.upload()
    try:
      storage1.load_sequences(None)
    except:
      pass
    else:
      self.fail("Expected load_sequences to discover the unexpected container")
  def test_summary_containers(self):
    """Test that summary containers are created and later used"""
    self.CONFIGURATION['key'] = 'test_summary_containers'
    self.CONFIGURATION['type'] = '__mock__'
    storage = Storage.create_storage(self.env, self.txn, 0,
      self.CONFIGURATION, None)
    storage.set_container_size(256)
    storage.create_sequence()
    start_summary_headers = storage.summary_headers_written
    # Create a container with many entries, to make sure a summary container
    # becomes due.
    container = storage.create_container()
    for i in range(10):
      data = "block %d" % i
      digest = Digest.dataDigest(data)
      if not container.can_add(data):
        container.finish_dump()
        container.upload()
        self.txn.commit()
        container = storage.create_container()
      container.add_block(digest, Container.CODE_DATA, data)
    end_summary_headers = storage.summary_headers_written

    # Check that a summary header was indeed created
    self.failUnless(start_summary_headers < end_summary_headers)
    
    # Recreate the storage from containers to see that summary container
    #    was created and loaded.
    self.CONFIGURATION['key'] = 'test_summary_containers'
    class DummyHandler:
      def report_new_container(self, container):
        print "Reporting new container", container.index
        container.load_header()
    storage = Storage.create_storage(self.env, self.txn, 1,
      self.CONFIGURATION, DummyHandler())
    storage.set_container_size(256)
    storage.create_sequence()
    print "HEADERS LOADED",
    print storage.headers_loaded_total,
    print storage.headers_loaded_from_summary,
    print storage.headers_loaded_from_storage
    self.assert_(storage.headers_loaded_from_summary > 0)
  def test_summary_container_recreated(self):
    """Test that if we ask the storage to recreate summary containers,
    and recreate it from containers where no summary containers actually
    existed, then the summary containers actually get created for the old
    sequences"""
    self.CONFIGURATION['key'] = 'test_summary_container_recreated'
    self.CONFIGURATION['type'] = '__mock__'
    storage = Storage.create_storage(self.env, self.txn, 0,
      self.CONFIGURATION, None)
    # Set a large container size, to make sure
    # summary container is not made.
    storage.set_container_size(1 << 20)
    storage.create_sequence()
    start_summary_headers = storage.summary_headers_written
    # Create a container with many entries, to make sure a summary container
    # becomes due.
    for i in range(10):
      data = "block %d" % i
      digest = Digest.dataDigest(data)
      container = storage.create_container()
      container.add_block(digest, Container.CODE_DATA, data)
      container.finish_dump()
      container.upload()
      self.txn.commit()
    end_summary_headers = storage.summary_headers_written
    # No summary headers made yet...
    self.assert_(start_summary_headers == end_summary_headers)

    # Now create a new storage, while making a summary container
    self.CONFIGURATION['generate_summary'] = 'true'
    class DummyHandler:
      def report_new_container(self, container):
        print "Reporting new container", container.index
        container.load_header()
    storage = Storage.create_storage(self.env, self.txn, 1,
      self.CONFIGURATION, DummyHandler())
    self.assert_(1 == storage.summary_headers_written)
    self.fail()
