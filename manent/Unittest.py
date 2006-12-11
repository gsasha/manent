import unittest

import testsuite.TestFormat
suite_Format = unittest.TestLoader().loadTestsFromTestCase(testsuite.TestFormat.TestFormat)

unittest.TextTestRunner(verbosity=2).run(suite_Format)
