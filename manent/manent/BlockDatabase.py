import base64
import sys, os

import Container

class BlockDatabase:
	def __init__(self, db_config, repository):
		self.db_config = db_config
		self.block_repository = repository

		# These two databases are scratch-only, so they don't need to reliably
		# survive through program restarts
		self.requested_data_blocks = self.db_config.get_scratch_database(
			".scratch-blocks")
		self.loaded_data_blocks = self.db_config.get_scratch_database(
			".scratch-blocks-data")
		self.cached_blocks = self.db_config.get_database(".blocks-data")
		self.block_type_db = self.db_config.get_database(".blocks-types")
		#
		# It is possible that the program was terminated before the scratch
		# cache was removed. In that case, it contains junk data
		#
		self.requested_data_blocks.truncate()
		self.loaded_data_blocks.truncate()
		
		self.containers = None
	def close(self):
		self.requested_data_blocks.close()
		self.loaded_data_blocks.close()
		self.cached_blocks.close()
		self.block_type_db.close()
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
	def add_block(self, digest, data, code):
		self.repository.add_block(digest, data, code)
		if code != Container.CODE_DATA:
			self.block_type_db[digest] = code
			self.cached_blocks[digest] = data
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
		if self.loaded_data_blocks.has_key(digest):
			data = self.loaded_data_blocks[digest]
			#
			# See if we can unload this block
			#
			if self.requested_blocks.has_key(digest):
				refcount = int(self.requested_data_blocks[digest])-1
				if refcount == 0:
					del self.requested_blocks[digest]
					del self.loaded_data_blocks[digest]
				else:
					self.requested_data_blocks[digest] = str(refcount)
			return data
		#
		# OK, block is not found anywhere. Load it from the container
		#
		block_type = self.get_block_type(digest)
		
		self.repository.load_block(digest, BlockLoadHandler(self))
		if self.block_type_db.has_key(digest):
			data = self.cached_blocks[digest]
		else:
			data = self.loaded_data_blocks[digest]
		return data
	def get_block_storage(self, digest):
		return self.repository.get_block_storage(digest)
	def get_block_type(self, digest):
		if self.block_type_db.has_key(digest):
			return int(self.block_type_db[digest])
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
