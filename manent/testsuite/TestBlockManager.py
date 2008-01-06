import base64
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
		found_container = None
		for b_digest, b_code, b_data, b_container in self.blocks:
			if digest == b_digest:
				found_container = b_container
		for b_digest, b_code, b_data, b_container in self.blocks:
			if handler.is_requested(b_digest, b_code):
				handler.loaded(b_digest, b_code, b_data)
				self.num_blocks_loaded += 1

class TestBlockManager(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseConfig()
		self.storage_manager = MockStorageManager()
	def add_block(self, bm, code, data):
		bm.add_block(Digest.dataDigest(data), code, data)
	def test_add_data_block_types(self):
		"""Test that blocks of different types can be added and restored"""
		bm = BlockManager.BlockManager(self.env, None, self.storage_manager)
		self.add_block(bm, Container.CODE_DATA, "aaa")
		self.add_block(bm, Container.CODE_DATA_PACKER, "bbb")
		self.add_block(bm, Container.CODE_DIR, "ccc")
		self.add_block(bm, Container.CODE_DIR_PACKER, "ddd")

		# bbb is not a data block. It should be there always
		self.assertEqual(bm.load_block(Digest.dataDigest("bbb")), "bbb")
		self.assertEqual(0, self.storage_manager.num_load_block_requests)

		# aaa is a data block. It is not available if not requested
		bm.request_block(Digest.dataDigest("aaa"))
		self.assertEqual(bm.load_block(Digest.dataDigest("aaa")), "aaa")
		self.assertEqual(1, self.storage_manager.num_load_block_requests)
		# ccc and ddd are non-data blocks, so they should be cached and
		# loading them needs not call the storage manager
		self.assertEqual(bm.load_block(Digest.dataDigest("ccc")), "ccc")
		self.assertEqual(1, self.storage_manager.num_load_block_requests)
		self.assertEqual(bm.load_block(Digest.dataDigest("ddd")), "ddd")
		self.assertEqual(1, self.storage_manager.num_load_block_requests)
	def test_block_requests(self):
		"""TODO: Test that block requests are handled correctly (i.e., the
		corresponding blocks are cached even if they are DATA ones"""
		pass
