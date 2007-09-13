import unittest

test_loader = unittest.TestLoader()

from testsuite.TestPacker import TestPacker
suite_Packer = test_loader.loadTestsFromTestCase(TestPacker)
import testsuite.TestIncrementTree
suite_ITree = test_loader.loadTestsFromTestCase(testsuite.TestIncrementTree.TestIncrementTree)
import testsuite.TestFormat
suite_Format = test_loader.loadTestsFromTestCase(testsuite.TestFormat.TestFormat)
import testsuite.TestBlock
suite_Block = test_loader.loadTestsFromTestCase(testsuite.TestBlock.TestBlock)
import testsuite.TestDatabase
suite_DB = test_loader.loadTestsFromTestCase(testsuite.TestDatabase.TestDatabase)
import testsuite.TestNodes
suite_Nodes = test_loader.loadTestsFromTestCase(testsuite.TestNodes.TestNodes)

suite = unittest.TestSuite([suite_Packer])
#suite = unittest.TestSuite([suite_Nodes, suite_ITree, suite_Format, suite_Block, suite_DB])
unittest.TextTestRunner(verbosity=2).run(suite)
