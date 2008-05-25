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
    bm.request_block(Digest.dataDigest("aaa"))
    bm.add_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    bm.add_block(Digest.dataDigest("bbb"), Container.CODE_DATA_PACKER, "bbb")
    bm.add_block(Digest.dataDigest("ccc"), Container.CODE_DIR, "ccc")
    bm.add_block(Digest.dataDigest("ddd"), Container.CODE_DIR_PACKER, "ddd")

    # Even though aaa was requested, it will be stored only if added by
    # handle_block().
    self.failIf(bm.has_block(Digest.dataDigest("aaa")))
    # bbb is not a data block. It should be there always
    self.assertEqual(bm.load_block(Digest.dataDigest("bbb")), "bbb")
    # ccc and ddd are non-data blocks, so they should be cached.
    self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
    self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
    
  def test_handle_block(self):
    # Test that blockks that have been added by handle_block() are later found.
    bm = BlockManager.BlockManager(self.env, None)

    # aaa is Data block. It is requested twice, so it should be available twice
    # in load_block.
    bm.request_block(Digest.dataDigest("aaa"))
    bm.request_block(Digest.dataDigest("aaa"))
    bm.handle_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    # bbb is Data block. It is not requested, so it should not be available at
    # all.
    bm.handle_block(Digest.dataDigest("bbb"), Container.CODE_DATA, "bbb")
    # ccc is non-data block. It is not requested, but it should be available.
    bm.handle_block(Digest.dataDigest("ccc"), Container.CODE_DIR, "ccc")
    # ddd is non-data block. It is requested, and it should be available.
    bm.request_block(Digest.dataDigest("ddd"))
    bm.handle_block(Digest.dataDigest("ddd"), Container.CODE_DATA_PACKER, "ddd")

    # Loading the blocks.
    for i in range(2):
      self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
    self.failIf(bm.has_block(Digest.dataDigest("aaa")))

    self.failIf(bm.has_block(Digest.dataDigest("bbb")))

    for i in range(5):
      self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
      self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
   
