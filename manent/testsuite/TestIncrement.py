import unittest

from manent.Increment import *
from Mock import *

class TestIncrement(unittest.TestCase):
	def test_load(self):
		repository = MockRepository()
		db = {}

		increment1 = Increment(repository,db)
		increment1.start(0,1,"test increment 1")
		increment1.finalize("aaaaaa")

		increment2 = Increment(repository,db)
		increment2.load(0,1)
		self.assertEqual(increment1.comment, increment2.comment)
		self.assertEqual(increment1.fs_digest, increment2.fs_digest)
		self.assertEqual(increment1.ctime, increment2.ctime)
		#self.assertEqual(increment1.erases_prev, increment2.erases_prev)
		#self.assertEqual(increment1,increment2)
