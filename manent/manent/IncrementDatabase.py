from Increment import *
import re

# TODO: reconstruction of IncrementDatabase

class IncrementDatabase:
	def __init__(self,repository,db):
		self.repository = repository
		self.db = db
		
		self.active_increment = None
		self.previous_increment = None

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
		increment_rexp = re.parse('Increment.([^\.]+).([^\.]+).finalized')
		for key,value in self.db:
			if key.startswith('Increment') and key.endswith('finalized'):
				match = re.match(key)
				storage_index = ascii_decode_int_varlen(match.group(1))
				index = ascii_decode_int_varlen(match.group(2))
				finalized = int(value)

				if not found_increments.has_key(storage_index):
					found_increments[storage_index] = []
				found_increments[storage_index].append((index,finalized))

		#
		# Decide which increments are going to be used for scan
		#
		selected_increment_fs_digests = []
		
		for storage_index,increments in found_increments:
			selected_increments = []
			for index,finalized in sorted(increments):
				if finalized:
					selected_increments = [index]
				else:
					selected_increments.append(index)
			for index in selected_increments:
				increment = Increment(self.repository,self.db)
				increment.load(storage_index,index)
				selected_increment_fs_digests.append(increment.get_fs_digest())

		#
		# Create the new active increment
		#
		active_storage = self.repository.get_active_storage_index()
		candidate_increments = found_increments[active_storage]
		if found_increments.has_key(active_storage):
			next_index = sorted(found_increments[active_storage])[-1]+1
		else:
			next_index = 0
			
		self.active_increment = Increment(self,self.repository,self.db)
		self.active_increment.start(storage_index,next_index,comment)

		return selected_increment_fs_digests
	
	def finalize_increment(self,digest):
		assert self.active_increment is not None

		if self.previous_increment is not None:
			self.active_increment.erase_previous(self.previous_increment)
		
		self.active_increment.finalize(digest)
		self.db["IncrementDB.last_completed"] = str(self.active_increment.get_index())
		self.previous_increment = None
		self.active_increment = None

	def dump_intermediate(self,digest):
		# TODO: kill the previous intermediate increment
		assert self.active_increment is not None
		self.active_increment.dump_intermediate(digest)
		
		if self.previous_increment is not None:
			self.active_increment.erase_previous(self.previous_increment)
		self.previous_increment = self.active_increment
		
		index = self.active_increment.get_index()
		comment = self.active_increment.get_comment()
		
		self.db["IncrementDB.last_intermediate"] = str(index)
		self.active_increment = Increment(self,self.repository,self.db)
		self.active_increment.start(index+1,comment)

	def reconstruct(self):
		class Handler:
			"""Handler reports all increment-related data"""
			def __init__(self,idb):
				self.idb = idb
			def block_loaded(self,digest,data,code):
				if code != CODE_INCREMENT_DESCRIPTOR:
					return
				increment = Increment(self.idb,self.idb.repository,self.idb.db)
				increment.reconstruct(digest)
		
		handler = Handler(self)
		self.block_database.reconstruct(handler)
