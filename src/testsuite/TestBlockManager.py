#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import sys
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.BlockManager as BlockManager
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
        handler.loaded(b_digest, b_code, b_data)
        self.num_blocks_loaded += 1
      else:
        pass

class TestBlockManager(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
  def tearDown(self):
    self.env = None
    Config.paths.clean_temp_area()
  def add_block(self, bm, code, data):
    bm.add_block(Digest.dataDigest(data), code, data)
  def test_add_block(self):
    # Test that blocks of different types can be added and restored.
    bm = BlockManager.BlockManager(self.env, None)
    bm.add_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    bm.add_block(Digest.dataDigest("bbb"), Container.CODE_DATA_PACKER, "bbb")
    bm.add_block(Digest.dataDigest("ccc"), Container.CODE_DIR, "ccc")
    bm.add_block(Digest.dataDigest("ddd"), Container.CODE_DIR_PACKER, "ddd")

    self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
    # bbb is not a data block. It should be there always
    self.assertEqual(bm.load_block(Digest.dataDigest("bbb")), "bbb")
    # ccc and ddd are non-data blocks, so they should be cached.
    self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
    self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
    
  def test_handle_block(self):
    # Test that blockks that have been added by handle_block() are later found.
    bm = BlockManager.BlockManager(self.env, None)

    bm.handle_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    bm.handle_block(Digest.dataDigest("bbb"), Container.CODE_DATA, "bbb")
    bm.handle_block(Digest.dataDigest("ccc"), Container.CODE_DIR, "ccc")
    bm.handle_block(Digest.dataDigest("ddd"), Container.CODE_DATA_PACKER, "ddd")

    # Loading the blocks.
    for i in range(5):
      self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
      self.assertEqual(bm.load_block(Digest.dataDigest("bbb")), "bbb")
      self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
      self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
  def test_epoch(self):
    # Test that when we increment the epoch, some blocks are gone
    bm = BlockManager.BlockManager(self.env, None)
    # There are 4 blocks: aaa, bbb, ccc, ddd.
    # - aaa is CODE_DATA, but is not used, so it must go away after a certain
    #   number of epochs
    # - bbb is CODE_DATA, and is used once, so it will not go away for a long
    #   time.
    # - ccc is CODE_DATA, and is used 10 times, so it will not go away for a
    #   very long time.
    # - ddd is CODE_DIR, so it is never supposed to go away, although it's never
    #   used.
    aaa_d = Digest.dataDigest("aaa")
    bbb_d = Digest.dataDigest("bbb")
    ccc_d = Digest.dataDigest("ccc")
    ddd_d = Digest.dataDigest("ddd")

    bm.add_block(aaa_d, Container.CODE_DATA, "aaa")
    bm.add_block(bbb_d, Container.CODE_DATA, "bbb")
    bm.add_block(ccc_d, Container.CODE_DATA, "ccc")
    bm.add_block(ddd_d, Container.CODE_DIR, "ddd")

    self.assert_(bm.has_block(aaa_d))
    self.assert_(bm.has_block(bbb_d))
    self.assert_(bm.has_block(ccc_d))
    self.assert_(bm.has_block(ddd_d))

    bm.load_block(bbb_d)
    for i in range(10):
      bm.load_block(ccc_d)

    for i in range(5):
      bm.increment_epoch()
    self.failIf(bm.has_block(aaa_d))
    self.assert_(bm.has_block(bbb_d))
    self.assert_(bm.has_block(ccc_d))
    self.assert_(bm.has_block(ddd_d))

    for i in range(100):
      bm.increment_epoch()
    self.failIf(bm.has_block(aaa_d))
    self.failIf(bm.has_block(bbb_d))
    self.assert_(bm.has_block(ccc_d))
    self.assert_(bm.has_block(ddd_d))

    for i in range(1000):
      bm.increment_epoch()
    self.failIf(bm.has_block(aaa_d))
    self.failIf(bm.has_block(bbb_d))
    self.failIf(bm.has_block(ccc_d))
    self.assert_(bm.has_block(ddd_d))
