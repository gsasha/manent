import unittest

import testsuite.TestFormat
suite_Format = unittest.TestLoader().loadTestsFromTestCase(testsuite.TestFormat.TestFormat)
unittest.TextTestRunner(verbosity=2).run(suite_Format)

import testsuite.TestBlock
suite_Block = unittest.TestLoader().loadTestsFromTestCase(testsuite.TestBlock.TestBlock)
unittest.TextTestRunner(verbosity=2).run(suite_Block)
