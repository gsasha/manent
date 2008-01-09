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

	def start_increment(self, comment):
		assert self.active_increment is None

		# TODO: algorithm:
		# - Read all the increments from the database
		#   - Filter to those that are:
		#     * one last completed increment in its storage
		#     * all intermediate increments for each storage
		#       that have not been erased
		found_increments = {}

		#
		# Scan the database of increments, recording the relevant
		# data to memory
		#
		increment_rexp = re.compile('Increment.([^\.]+).([^\.]+).finalized')
		for key, value in self.config_db.iteritems():
			if key.startswith('Increment') and key.endswith('finalized'):
				match = increment_rexp.match(key)
				storage_index = IE.ascii_decode_int_varlen(match.group(1))
				index = IE.ascii_decode_int_varlen(match.group(2))
				finalized = int(value)

				if not found_increments.has_key(storage_index):
					found_increments[storage_index] = []
				found_increments[storage_index].append((index,finalized))

		#
		# Decide which increments are going to be used for scan
		#
		selected_increment_fs_digests = []
		
		for storage_index, increments in found_increments.iteritems():
			selected_increments = []
			for index, finalized in sorted(increments):
				if finalized:
					selected_increments = [index]
				else:
					selected_increments.append(index)
			for index in selected_increments:
				increment = Increment.Increment(self.block_manager, self.config_db)
				increment.load(storage_index, index)
				selected_increment_fs_digests.append(increment.get_fs_digest())

		#
		# Create the new active increment
		#
		storage_index = self.storage_manager.get_active_storage_index()
		if found_increments.has_key(storage_index):
			last_index, last_finalized = sorted(found_increments[storage_index])[-1]
			next_index = last_index+1
		else:
			next_index = 0
			
		self.active_increment = Increment.Increment(self.block_manager, self.config_db)
		self.active_increment.start(storage_index,next_index,comment)

		return selected_increment_fs_digests
	
	def finalize_increment(self,digest):
		assert self.active_increment is not None

		inc_digest = self.active_increment.finalize(digest)
		self.active_increment = None
		return inc_digest
		
	def dump_intermediate(self, digest):
		"""
		Replace the data of this increment with a new digest.
		The current increment remains, with the same index.
		"""
		assert self.active_increment is not None
		
		inc_digest = self.active_increment.dump_intermediate(digest)

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
