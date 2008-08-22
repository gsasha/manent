#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#


import logging
import unittest

#logging.basicConfig(level=logging.DEBUG)

test_loader = unittest.TestLoader()

from testsuite.TestPacker import TestPacker
suite_Packer = test_loader.loadTestsFromTestCase(TestPacker)

from testsuite.TestContainer import TestContainer
suite_Container = test_loader.loadTestsFromTestCase(TestContainer)

from testsuite.TestIncrement import TestIncrement
suite_Increment = test_loader.loadTestsFromTestCase(TestIncrement)

from testsuite.TestFormat import TestFormat
suite_Format = test_loader.loadTestsFromTestCase(TestFormat)

from testsuite.TestDatabase import TestDatabase
suite_DB = test_loader.loadTestsFromTestCase(TestDatabase)

from testsuite.TestNodes import TestNodes
suite_Nodes = test_loader.loadTestsFromTestCase(TestNodes)

from testsuite.TestExclusionProcessor import TestExclusionProcessor
suite_Exclusion = test_loader.loadTestsFromTestCase(TestExclusionProcessor)

from testsuite.TestStorage import TestStorage
suite_Storage = test_loader.loadTestsFromTestCase(TestStorage)

from testsuite.TestStorageManager import TestStorageManager
suite_StorageManager = test_loader.loadTestsFromTestCase(TestStorageManager)

from testsuite.TestBlockManager import TestBlockManager
suite_BlockManager = test_loader.loadTestsFromTestCase(TestBlockManager)

from testsuite.TestBlockSequencer import TestBlockSequencer
suite_BlockSequencer = test_loader.loadTestsFromTestCase(TestBlockSequencer)

suite = unittest.TestSuite([
	suite_Packer,
	suite_Container,
	suite_Nodes,
	suite_Increment,
	suite_DB,
	suite_Format,
	suite_Exclusion,
	suite_Storage,
	suite_StorageManager,
	suite_BlockManager,
  suite_BlockSequencer,
	])

if __name__ == "__main__":
  unittest.TextTestRunner(verbosity=2).run(suite)
