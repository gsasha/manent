import base64
import sys, os

import Container

class BlockDatabase:
	def __init__(self, db_config, storage_manager):
		self.db_config = db_config
		self.storage_manager = storage_manager

		# These two databases are scratch-only, so they don't need to reliably
		# survive through program restarts
		self.requested_blocks = self.db_config.get_scratch_database(
			".scratch-requested-blocks")
		self.loaded_blocks = self.db_config.get_scratch_database(
			".scratch-data-blocks")
		self.cached_blocks = self.db_config.get_database(".cached-blocks")
		self.block_types = self.db_config.get_database(".block-types")
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
		self.block_types.close()
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
		self.repository.add_block(digest, code, data)
		if code != Container.CODE_DATA:
			# We store the block code only for blocks that are not DATA.
			# The DATA blocks are the majority, and so  by not storing them,
			# we save space in the database.
			self.block_types[digest] = code
			self.cached_blocks[digest] = data
	def load_block(self, digest):
		"""
		Actually perform loading of the block. Assumes that the block
		was reported by request_block, and was loaded not more times than
		it was requested.
		"""
		if not self.cached_blocks.has_key(digest) and\
		   not self.loaded_blocks.has_key(digest):
			self.storage_manager.load_block(digest, BlockLoadHandler(self))

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
		#
		# OK, block is not found anywhere. Load it from the container
		#
		block_type = self.get_block_type(digest)
		
		return data
	def get_block_type(self, digest):
		if self.block_types.has_key(digest):
			return int(self.block_types[digest])
		else:
			return Container.CODE_DATA
		
class BlockLoadHandler:
	"""Callback class used by repository to return loaded blocks
	   to the database"""
	def __init__(self, blocks_db):
		self.blocks_db = blocks_db
	def is_block_necessary(self, digest, code):
		if self.requested_blocks.has_key(digest):
			# Data blocks must be specifically requested
			return True
		if code != Container.CODE_DATA:
			# Other kinds of blocks are cached always
			return True
		return False
	def block_loaded(self, digest, data, code):
		# TODO(gsasha): I don't understand the logic - when does the block
		# go to cached_blocks?
		if self.blocks_db.has_key(digest):
			self.cached_blocks[digest] = data
		else:
			self.loaded_data_blocks[digest] = data
