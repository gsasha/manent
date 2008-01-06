import logging
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.BlockManager as BlockManager
import manent.utils.Digest as Digest

# For the purposes of this testing, we don't care that storage manager
# has smart restoring and multi-sequence, multi-storage capabilities.
# For the BlockManager, it just can store and load blocks grouped by
# containers
class MockStorageManager:
	def __init__(self):
		self.blocks = []
		self.container = 0
		self.num_load_block_requests = 0
		self.num_blocks_loaded = 0
	def new_container(self):
		self.container += 1
	def add_block(self, digest, code, data):
		self.blocks.append((digest, code, data, self.container))
	def load_block(self, digest, handler):
		self.num_load_block_requests += 1
		for digest, code, data, container in self.blocks:
			if handler.is_requested(digest, code):
				handler.loaded(digest, code, data)
				self.num_blocks_loaded += 1

class TestBlockManager(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.storage_manager = MockStorageManager()
	def add_block(self, code, data):
		self.storage_manager.add_block(Digest.dataDigest(data), code, data)
	def test_add_data_block_types(self):
		"""Test that blocks of different types can be added and restored"""
		bm = BlockManager.BlockManager(self.env, None, self.storage_manager)
		self.add_block(Container.CODE_DATA, "aaa")
		self.add_block(Container.CODE_DATA_PACKER, "bbb")
		self.add_block(Container.CODE_DIR, "ccc")
		self.add_block(Container.CODE_DIR_PACKER, "ddd")

		bm.request_block(Digest.dataDigest("aaa"))
		self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
	def test_data_block_caching(self):
		"""Test that DATA blocks are uncached by default, and the others
		are cached"""
		self.fail()
	def test_block_requests(self):
		"""Test that block requests are handled correctly (i.e., the
		corresponding blocks are cached even if they are DATA ones"""
		self.fail()
