import logging
import os
import shutil
import tempfile
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.StorageManager as StorageManager
import manent.utils.Digest as Digest

class TestStorageManager(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseManager()
		self.txn = Database.TransactionHandler(self.env)
	def tearDown(self):
		# Clean up the state, to make sure tests don't interfere.
		Storage.MemoryStorage.files = {}
	def test_add_storage(self):
		"""Test that adding a storage creates (and recreates) it correctly"""
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		storage_index = storage_manager.add_storage(
			{'type': '__mock__', 'password': 'kuku', 'key': ''}, None)
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		seq_id1 = storage_manager.get_active_sequence_id()
		# Recreate the storage_manager and add another block to it
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		block = "some other strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		seq_id2 = storage_manager.get_active_sequence_id()
		self.assertEqual(seq_id1, seq_id2)
		#for k,v in self.config_db.iteritems():
			#print k, " : ", v
		#for k,v in self.block_db.iteritems():
			#print k, " : ", v
	def test_add_block(self):
		"""Test that if blocks are added, they are available for loading back"""
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		storage_index = storage_manager.add_storage(
			{'type': '__mock__', 'password': 'kuku', 'key': ''}, None)
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		# Recreate the storage and read the block back
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		class Handler:
			def __init__(self):
				self.blocks = {}
			def is_requested(self, digest, code):
				return True
			def loaded(self, digest, code, data):
				self.blocks[(digest, code)] = data
		handler = Handler()
		storage_manager.load_blocks_for(block_digest, handler)
		self.assertEqual({(block_digest, Container.CODE_DATA): block},
			handler.blocks)
	def test_rescan_storage(self):
		"""Test that new sequences appearing from outside are discovered"""
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		storage_index = storage_manager.add_storage(
			{'type': '__mock__', 'password': 'kuku', 'key': ''}, None)
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		# Create second storage manager with a different db, but on the same storage
		# (mock shares all the files), and see that it sees the block from the first one.
		class Handler:
			def __init__(self):
				self.blocks = {}
			def is_requested(self, digest, code):
				return True
			def loaded(self, digest, code, data):
				self.blocks[(digest, code)] = data
		handler = Handler()
		
		env2 = Database.PrivateDatabaseManager()
		txn2 = Database.TransactionHandler(env2)
		storage_manager2 = StorageManager.StorageManager(env2, txn2)
		storage_manager2.load_storages(None)
		storage_index2 = storage_manager2.add_storage(
		    {'type': '__mock__', 'password': 'kuku', 'key': ''}, None)
		storage_manager2.make_active_storage(storage_index2)
		storage_manager2.load_blocks_for(block_digest, handler)
		self.assertEqual({(block_digest, Container.CODE_DATA): block},
			handler.blocks)
	def test_base_storage(self):
		"""Test that base storage works"""
		# First storage manager. This will be the base.
		logging.debug("creating first storage manager")
		storage_manager = StorageManager.StorageManager(self.env, self.txn)
		storage_manager.load_storages(None)
		storage_index = storage_manager.add_storage(
			{'type': '__mock__', 'password': 'kuku', 'key': 'a'}, None)
		storage_manager.make_active_storage(storage_index)
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		storage_manager.add_block(block_digest, Container.CODE_DATA, block)
		storage_manager.flush()
		# Second storage manager with a different db, and on a different storage
		# and see that it sees the block from the base one.
		logging.debug("creating second storage manager")
		class Handler:
			def __init__(self):
				self.blocks = {}
			def is_requested(self, digest, code):
				return True
			def loaded(self, digest, code, data):
				self.blocks[(digest, code)] = data
		handler = Handler()
		
		env2 = Database.PrivateDatabaseManager()
		txn2 = Database.TransactionHandler(env2)
		storage_manager2 = StorageManager.StorageManager(env2, txn2)
		storage_manager2.load_storages(None)
		storage_index2 = storage_manager2.add_storage(
		    {'type': '__mock__', 'password': 'kuku', 'key': 'a'}, None)
		storage_manager2.make_active_storage(storage_index2)
		storage_manager2.load_blocks_for(block_digest, handler)
		self.assertEqual({(block_digest, Container.CODE_DATA): block},
			handler.blocks)
	def test_container(self):
		"""Test that containers are created when necessary"""
		# TODO: consider doing this test
		pass
