import os
import shutil
import tempfile
import unittest

import manent.Database as Database
import manent.Repository as Repository

class TestRepository(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.config_db = self.env.get_database_btree("config", None)
		self.block_db = self.env.get_database_btree("block_db", None)
	def test_add_new_storage(self):
		"""Test that adding a new storage creates it correctly"""
		repository = Repository.Repository(self.config_db, self.block_db)
		#self.fail()
	def test_add_existing_storage(self):
		"""Test that adding an existing storage imports and maps the storage correctly"""
		self.fail()
	def test_rescan_storage(self):
		"""Test that new sequences appearing from outside are discovered"""
		self.fail()
	def test_add_block(self):
		"""Test that if blocks are added, they are available for loading back"""
		self.fail()
	def test_container(self):
		"""Test that containers are created when necessary"""
		self.fail()
