import logging
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.StorageManager as StorageManager
import manent.utils.Digest as Digest

class TestStorageManager(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.config_db = self.env.get_database_btree("config", None)
		self.block_db = self.env.get_database_btree("block_db", None)
	def tearDown(self):
		# Clean up the state, to make sure tests don't interfere.
		Storage.MemoryStorage.files = {}
	def test_add_storage(self):
		"""Test that adding a storage creates (and recreates) it correctly"""
		storage_manager = StorageManager.StorageManager(self.config_db,
			self.block_db)
		storage_manager.load_storages(None)
		storage_index = storage_manager.add_storage("__mock__",
			{'password': 'kuku', 'key': ''}, None)
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		seq_id1 = storage_manager.get_active_sequence_id()
		# Recreate the storage_manager and add another block to it
		storage_manager = StorageManager.StorageManager(self.config_db,
			self.block_db)
