#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import os
import shutil
import tempfile
import unittest

import manent.Config as Config
import manent.Container as Container
import manent.Database as Database
import manent.Sequence as Sequence
import manent.utils.Digest as Digest

class MockStorage:
	def __init__(self, config_db):
		self.config_db = config_db
	def _key(self, suffix):
		return "sequence_key." + suffix

class TestSequence(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseManager()
		self.txn = Database.TransactionHandler(self.env)
		self.config_db = self.env.get_database_btree("TestSequenceConfigDB",
			None, None)
		self.storage = MockStorage(self.config_db)
		#print "tmp path=", self.scratch_path
	def tearDown(self):
		pass
	def test_get_next_index(self):
		sequence = Sequence.Sequence(self.storage, "sequence_a", False)
		self.assertEquals(0, sequence.get_next_index())
		self.assertEquals(1, sequence.get_next_index())
		self.assertEquals(2, sequence.get_next_index())
		# Recreate the sequence, see that sequence numbers continue
		sequence = Sequence.Sequence(self.storage, "sequence_a", False)
		self.assertEquals(3, sequence.get_next_index())
