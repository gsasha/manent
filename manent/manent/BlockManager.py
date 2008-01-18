import base64
import sys, os

import Container

class BlockManager:
	def __init__(self, db_manager, txn_handler, storage_manager):
		self.db_manager = db_manager
		self.txn_handler = txn_handler
		self.storage_manager = storage_manager

		# These two databases are scratch-only, so they don't need to reliably
		# survive through program restarts
		self.requested_blocks = self.db_manager.get_scratch_database(
			"scratch-requested-blocks", None)
		self.loaded_blocks = self.db_manager.get_scratch_database(
			"scratch-data-blocks", None)
		self.cached_blocks = self.db_manager.get_database("cached-blocks",
			None, self.txn_handler)
		self.block_codes = self.db_manager.get_database("block-types",
			None, self.txn_handler)
		#
		# It is possible that the program was terminated before the scratch
		# cache was removed. In that case, it contains junk data
		#
		self.requested_blocks.truncate()
		self.loaded_blocks.truncate()
	def close(self):
		self.requested_blocks.close()
		self.loaded_blocks.close()
		self.cached_blocks.close()
		self.block_codes.close()
	#
	# Methods for the user side of the cache
	#
	def request_block(self, digest):
		"""
		Used for preprocessing, to make all the future needed blocks known -
		this is to avoid reloading containers unnecessarily.
		"""
		if self.requested_blocks.has_key(digest):
			self.requested_blocks[digest] = str(
				int(self.requested_blocks[digest]) + 1)
		else:
			self.requested_blocks[digest] = "1"
	def add_block(self, digest, code, data):
		if code != Container.CODE_DATA:
			# We store the block code only for blocks that are not DATA.
			# The DATA blocks are the majority, and so  by not storing them,
			# we save space in the database.
			self.cached_blocks[digest] = data
	def block_saved(self, digest, code):
		# We wait for storage manager to report that the block has been saved,
		# since its saving might get delayed if the block is put to an aside
		# container.
		# TODO: make use of this
		if code != Container.CODE_DATA:
			self.block_codes[digest] = code
	def has_block(self, digest):
		# It is important to use block_codes here, since they are filled up
		# only when the block is saved (which is not immediate for aside
		# blocks).
		return self.block_codes.has_key(digest) or\
			self.loaded_blocks.has_key(digest)
	def load_block(self, digest):
		"""
		Actually perform loading of the block. Assumes that the block
		was reported by request_block, and was loaded not more times than
		it was requested.
		"""
		if self.cached_blocks.has_key(digest):
			#
			# Blocks that sit in self.cached_blocks are never unloaded
			#
			return self.cached_blocks[digest]
		if self.loaded_blocks.has_key(digest):
			data = self.loaded_blocks[digest]
			#
			# See if we can unload this block
			#
			if self.requested_blocks.has_key(digest):
				refcount = int(self.requested_blocks[digest])-1
				if refcount == 0:
					del self.requested_blocks[digest]
					del self.loaded_blocks[digest]
				else:
					self.requested_blocks[digest] = str(refcount)
			return data
		raise Exception("Block neither cached nor loaded!!!")
	def get_block_code(self, digest):
		if self.block_codes.has_key(digest):
			return int(self.block_codes[digest])
		else:
			return Container.CODE_DATA
		
class BlockLoadHandler:
	"""Callback class used by repository to return loaded blocks
	   to the database"""
	def __init__(self, block_manager):
		self.block_manager = block_manager
	def is_requested(self, digest, code):
		if code != Container.CODE_DATA:
			# Other kinds of blocks are cached always
			return True
		if self.block_manager.requested_blocks.has_key(digest):
			# Data blocks must be specifically requested
			return True
		return False
	def loaded(self, digest, code, data):
		# All non-DATA blocks go to cache. These blocks are identified
		# by having their code in the block_codes database
		if self.block_manager.block_codes.has_key(digest):
			self.block_manager.cached_blocks[digest] = data
		else:
			self.block_manager.loaded_blocks[digest] = data
