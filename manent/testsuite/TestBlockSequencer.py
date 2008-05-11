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
  def container_written(self, container):
    # Do nothing - unlike real StorageManager, we don't register the blocks
    # anywhere.
    pass

class TestBlockSequencer(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.storage_manager = MockStorageManager()
  def testi_clean_start(self):
    # Check that if BlockSequencer is started cleanly, it is initialized
    # correctly.
    bs = BlockSequencer.BlockSequencer(self.env, self.txn, self.storage_manager)
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
    bs = BlockSequencer.BlockSequencer(self.env, self.txn, self.storage_manager)
    self.assertEquals(1, bs.get_aside_blocks_num())
    self.assertEquals(len(block), bs.get_aside_blocks_size())
    bs.close()

  def test_container_created(self):
    # Check that if blocks are added sufficiently many times, a new container
    # will be created.
    bs = BlockSequencer.BlockSequencer(self.env, self.txn, self.storage_manager)
    self.assertEquals(0, bs.num_containers_created)
    for i in range(5000):
      block = "A" * 100 + str(i)
      logging.debug("Adding block %d: %s" % (i, block))
      bs.add_block(Digest.dataDigest(block), Container.CODE_DATA, block)
    # First container is created on the first block, so we need at least another
    # one to be created to know that the first one was closed.
    self.assert_(1 < bs.num_containers_created)

  def test_add_aside_block(self):
    # Check that if we add an aside block, it is not written immediately.
    self.fail()

  def test_add_many_aside_blocks(self):
    # Check that if aside blocks are added sufficiently many times, they will
    # eventually be written to a container.
    self.fail()

  def test_flush(self):
    # Check that if flush() is called, all the current aside blocks are written
    # out (but not piggybacking blocks!)
    self.fail()

  def test_piggybacking_block(self):
    # Check that piggybacking blocks are created when necessary.
    self.fail()

