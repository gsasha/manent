import os
import shutil
import tempfile
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.Repository as Repository
import manent.utils.Digest as Digest

class TestRepository(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.config_db = self.env.get_database_btree("config", None)
		self.block_db = self.env.get_database_btree("block_db", None)
	def test_add_storage(self):
		"""Test that adding a storage creates (and recreates) it correctly"""
		repository = Repository.Repository(self.config_db, self.block_db)
		storage_index = repository.add_storage("__mock__", {'password': 'kuku'})
		repository.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		repository.add_block(block_digest, block, Container.CODE_DATA)
		repository.flush()
		#self.fail()
	def test_add_existing_storage(self):
		"""Test that adding an existing storage imports and maps the storage correctly"""
		self.fail()
	def test_rescan_storage(self):
		"""Test that new sequences appearing from outside are discovered"""
		self.fail()
	def test_base_storage(self):
		"""Test that base storage works"""
		self.fail()
	def test_add_block(self):
		"""Test that if blocks are added, they are available for loading back"""
		self.fail()
	def test_container(self):
		"""Test that containers are created when necessary"""
		self.fail()
