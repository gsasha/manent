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

class TestBlockManager(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.storage_manager = MockStorageManager()
  def test1(self):
    pass

  def test2(self):
    pass
