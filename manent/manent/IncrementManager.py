import base64
import re

import Increment
import utils.IntegerEncodings as IE

# TODO: reconstruction of IncrementManager

class IncrementManager:
	def __init__(self, db_manager, txn_handler, block_manager, storage_manager):
		self.block_manager = block_manager
		self.storage_manager = storage_manager
		self.config_db = db_manager.get_database_btree("config.db", "increments",
			txn_handler)
		
		self.active_increment = None
		self.previous_increment = None

	def close(self):
		self.config_db.close()

	def get_increments(self):
		return Increment.Increment.get_increments()
		
	def start_increment(self, comment):
		assert self.active_increment is None

		increments = self.get_increments()

		#
		# Create the new active increment
		#
		storage_index = self.storage_manager.get_active_storage_index()
		if increments.has_key(storage_index):
			last_index, last_finalized = sorted(increments[storage_index])[-1]
			next_index = last_index + 1
		else:
			next_index = 0

		self.active_increment = Increment.Increment(self.block_manager, self.config_db)
		self.active_increment.start(storage_index, next_index, comment)

		last_increment = Increment.Increment(self.block_manager, self.config_db)
		last_increment.load()
		return last_increment.get_fs_digest()

	def finalize_increment(self, digest):
		assert self.active_increment is not None

		print "Finalizing increment", self.active_increment.index, "to", base64.b64encode(digest)
		inc_digest = self.active_increment.finalize(digest)
		self.active_increment = None
		return inc_digest

	def reconstruct(self):
		class Handler:
			"""Handler reports all increment-related data"""
			def __init__(self, increment_manager):
				self.increment_manager = increment_manager
			def block_loaded(self, digest, data, code):
				if code != CODE_INCREMENT_DESCRIPTOR:
					return
				increment = Increment.Increment(self.increment_manager,
					self.increment_manager.block_manager, self.increment_manager.db)
				increment.reconstruct(digest)
		
		handler = Handler(self)
		self.block_manager.reconstruct(handler)
