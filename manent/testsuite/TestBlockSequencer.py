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
  def new_container(self):
    self.container += 1
  def add_block(self, digest, code, data):
    logging.debug("adding block %s %s %s" %
        (base64.b64encode(digest), Container.code_name(code), data))
    self.block_manager.add_block(digest, code, data)
    self.blocks.append((digest, code, data, self.container))
  def load_blocks_for(self, digest, handler):
    self.num_load_block_requests += 1
    found_container = None
    for b_digest, b_code, b_data, b_container in self.blocks:
      if digest == b_digest:
        found_container = b_container
    for b_digest, b_code, b_data, b_container in self.blocks:
      if handler.is_requested(b_digest, b_code):
        logging.debug("block requested: %s %s" %
            (base64.b64encode(b_digest), Container.code_name(b_code)))
        handler.loaded(b_digest, b_code, b_data)
        self.num_blocks_loaded += 1
      else:
        logging.debug("block not requested: %s %s" %
            (base64.b64encode(b_digest), Container.code_name(b_code)))

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
  def test_load(self):
    # Check that if BlockSequencer is started the second time, all its status is
    # preserved.
    self.fail()
    
  def test_container_created(self):
    # Check that if blocks are added sufficiently many times, a new container
    # will be created.
    self.fail()

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

