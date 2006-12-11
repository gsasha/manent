import unittest

import unit_test.TestFormat
suite_Format = unittest.TestLoader().loadTestsFromTestCase(test.TestFormat.TestFormat)

unittest.TextTestRunner(verbosity=2).run(suite_Format)
