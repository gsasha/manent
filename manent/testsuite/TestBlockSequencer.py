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

import manent.Database as Database

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
class TestBlockManager(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.storage_manager = MockStorageManager()
  def test1(self):
    pass

  def test2(self):
    pass
