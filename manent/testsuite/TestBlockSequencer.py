#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import sys
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.BlockManager as BlockManager
import manent.BlockSequencer as BlockSequencer
import manent.Container as Container
import manent.Database as Database
import manent.utils.Digest as Digest
import Mock

# For the purposes of this testing, we don't care that storage manager
# has smart restoring and multi-sequence, multi-storage capabilities.
# For the BlockManager, it just can store and load blocks grouped by
# containers
class MockStorageManager:
  def __init__(self):
    self.blocks = []
    self.container = 0
    self.num_load_block_requests = 0
    self.num_blocks_loaded = 0
    self.storage = Mock.MockStorage(password="kakamaika")
  def add_block(self, digest, code, data):
    logging.debug("adding block %s %s %s" %
        (base64.b64encode(digest), Container.code_name(code), data))
    self.block_manager.add_block(digest, code, data)
    self.blocks.append((digest, code, data, self.container))
  def create_container(self):
    return self.storage.create_container()
  def get_container(self, index):
    return self.storage.get_container(index)
  def container_written(self, container):
    # Do nothing - unlike real StorageManager, we don't register the blocks
    # anywhere.
    pass

class TestBlockSequencer(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.storage_manager = MockStorageManager()
    self.block_manager = BlockManager.BlockManager(
        self.env, self.txn, self.storage_manager)
  def tearDown(self):
    self.storage_manager = None
    self.block_manager = None
    self.env = None
    self.txn = None
  def test_clean_start(self):
    # Check that if BlockSequencer is started cleanly, it is initialized
    # correctly.
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    self.assertEquals(0, bs.get_aside_blocks_num())
    self.assertEquals(0, bs.get_aside_blocks_size())
    self.assertEquals(0, bs.get_piggyback_headers_num())

    block = "kukumuku"
    bs.add_block(Digest.dataDigest(block), Container.CODE_DIR, block)
    self.assertEquals(1, bs.get_aside_blocks_num())
    self.assertEquals(len(block), bs.get_aside_blocks_size())
    bs.close()

    # Check that if BlockSequencer is started the second time, all the state is
    # preserved.
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    self.assertEquals(1, bs.get_aside_blocks_num())
    self.assertEquals(len(block), bs.get_aside_blocks_size())
    bs.close()

  def test_container_created(self):
    # Check that if blocks are added sufficiently many times, a new container
    # will be created.
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    self.assertEquals(0, bs.num_containers_created)
    for i in range(5000):
      block = os.urandom(500) + str(i)
      logging.debug("Adding block %d: %s" % (i, block))
      bs.add_block(Digest.dataDigest(block), Container.CODE_DATA, block)
    # First container is created on the first block, so we need at least another
    # one to be created to know that the first one was closed.
    self.assert_(1 < bs.num_containers_created)

  def test_add_many_aside_blocks(self):
    # Check that if aside blocks are added sufficiently many times, they will
    # eventually be written to a container.
    self.storage_manager.storage.max_container_size = 512 * 1024
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    # All these blocks sit aside, and are not inserted into a container until a
    # normal block is inserted.
    for i in range(2000):
      block = os.urandom(1024)
      digest = Digest.dataDigest(block)
      logging.debug("Adding aside block %d: %d" % (i, len(block)))
      self.block_manager.add_block(digest, Container.CODE_DIR, block)
      bs.add_block(digest, Container.CODE_DIR, block)
    self.assertEquals(0, bs.num_containers_created)
    # Now insert a DATA block and observe that at least two containers have been
    # created - this is because the aside blocks have been pushed.
    # Dummy block must be larger than the aside block, otherwise it might fit
    # in the container which refused the aside block.
    block = os.urandom(2048)
    digest = Digest.dataDigest(block)
    bs.add_block(digest, Container.CODE_DATA, block)
    self.assert_(1 < bs.num_containers_created)

  def test_flush_empty(self):
    # Check that we can call flush() when nothing was added.
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    bs.flush()
    self.assertEquals(0, bs.num_containers_created)

  def test_flush(self):
    # Check that if flush() is called, all the current aside blocks are written
    # out (but not piggybacking blocks!)
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    block = "d" * 500
    digest = Digest.dataDigest(block)
    self.block_manager.add_block(digest, Container.CODE_DIR, block)
    bs.add_block(digest, Container.CODE_DIR, block)
    bs.flush()
    self.assertEquals(1, bs.num_containers_created)

  def test_piggybacking_block(self):
    # Check that piggybacking blocks are created when necessary.
    self.storage_manager.storage.max_container_size = 1000 * 1024
    bs = BlockSequencer.BlockSequencer(
        self.env, self.txn, self.storage_manager, self.block_manager)
    for i in range(20000):
      # We need to make sure block doesn't compress well, otherwise
      # the container will never end.
      block = os.urandom(25000) + str(i)
      digest = Digest.dataDigest(block)
      bs.add_block(digest, Container.CODE_DATA, block)
      logging.debug("Added block %d. Number of containers %d" %
          (i, bs.num_containers_created))
      if bs.num_containers_created == 6:
        # Container with index 4 must contain piggybacking headers.
        container = bs.current_open_container
        assert container.get_index() == 5
        # We want to inspect the previous container
        break
    container = self.storage_manager.get_container(4)
    class CheckHandler:
      def __init__(self):
        self.num_piggyback_headers = 0
      def is_requested(self, digest, code):
        if code == Container.CODE_HEADER:
          self.num_piggyback_headers += 1
        return False
    ch = CheckHandler()
    container.load_blocks(ch)
    self.assertEquals(4, ch.num_piggyback_headers)


