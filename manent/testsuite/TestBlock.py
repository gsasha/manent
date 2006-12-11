import unittest
import random
from cStringIO import StringIO

from manent.Block import *
from manent.VersionConfig import *

class MockBackup:
	def __init__(self):
		self.config = VersionConfig()
		self.blocks_db = {}

class TestBlock(unittest.TestCase):
	def setUp(self):
		self.backup = MockBackup()
	def test_block(self):
		block1 = Block(self.backup,"1111")
		containers = [10,20,30]
		for c in containers:
			block1.add_container(c)
		block1.save()

		block1 = Block(self.backup,"1111")
		self.assertEqual(block1.containers, containers)
