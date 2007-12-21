import os
import shutil
import tempfile
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.StorageManager as StorageManager
import manent.utils.Digest as Digest

class TestStorageManager(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.config_db = self.env.get_database_btree("config", None)
		self.block_db = self.env.get_database_btree("block_db", None)
	def test_add_storage(self):
		"""Test that adding a storage creates (and recreates) it correctly"""
		storage_manager = StorageManager.StorageManager(self.config_db, self.block_db)
		storage_index = storage_manager.add_storage("__mock__", {'password': 'kuku'})
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, block, Container.CODE_DATA)
		storage_manager.flush()
		for k,v in self.config_db.iteritems():
			print k, " : ", v
		# Recreate the storage_manager and add another block to it
		storage_manager = StorageManager.StorageManager(self.config_db, self.block_db)
		block = "some other strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, block, Container.CODE_DATA)
		storage_manager.flush()
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
