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
    #print "-------------- adding block", base64.b64encode(digest), code, data
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
        #print "--------- block", base64.b64encode(b_digest), b_code, "requested"
        handler.loaded(b_digest, b_code, b_data)
        self.num_blocks_loaded += 1
      else:
        #print "--------- block", base64.b64encode(b_digest), b_code, "not requested"
        pass

class TestBlockManager(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.storage_manager = MockStorageManager()
  def add_block(self, bm, code, data):
    bm.add_block(Digest.dataDigest(data), code, data)
  def test_add_data_block_types(self):
    """Test that blocks of different types can be added and restored"""
    bm = BlockManager.BlockManager(self.env, None, self.storage_manager)
    sm = self.storage_manager
    sm.block_manager = bm
    sm.add_block(Digest.dataDigest("aaa"), Container.CODE_DATA, "aaa")
    sm.add_block(Digest.dataDigest("bbb"), Container.CODE_DATA_PACKER, "bbb")
    sm.add_block(Digest.dataDigest("ccc"), Container.CODE_DIR, "ccc")
    sm.add_block(Digest.dataDigest("ddd"), Container.CODE_DIR_PACKER, "ddd")

    # bbb is not a data block. It should be there always
    self.assertEqual(bm.load_block(Digest.dataDigest("bbb")), "bbb")
    self.assertEqual(0, self.storage_manager.num_load_block_requests)

    # aaa is a data block. It is not available if not requested
    bm.request_block(Digest.dataDigest("aaa"))
    class Handler:
      def is_requested(self, digest, code):
        return BlockManager.is_cached(code)
      def loaded(self, digest, code, data):
        return
    self.storage_manager.load_blocks_for(Digest.dataDigest("aaa"), Handler())
    self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
    self.assertEqual(1, self.storage_manager.num_load_block_requests)
    # ccc and ddd are non-data blocks, so they should be cached and
    # loading them needs not call the storage manager
    self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
    self.assertEqual(1, self.storage_manager.num_load_block_requests)
    self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
    self.assertEqual(1, self.storage_manager.num_load_block_requests)
  def test_block_requests(self):
    """TODO: Test that block requests are handled correctly (i.e., the
    corresponding blocks are cached even if they are DATA ones"""
    pass
