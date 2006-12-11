import unittest

import test.TestFormat
suite_Format = unittest.TestLoader().loadTestsFromTestCase(test.TestFormat.TestFormat)

unittest.TextTestRunner(verbosity=2).run(suite_Format)
