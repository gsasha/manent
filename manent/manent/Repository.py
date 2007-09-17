import Container

class Repository:
	"""Handles the moving of blocks to and from storages.

	Input: a stream of blocks
	Creates containers and sends them to storage
	"""
	def __init__(self,db_config,storages,active_storage_index):
		self.db_config = db_config
		self.block_container_db = self.db_config.get_database(".block-container")
		self.current_open_container = None
		#
		# All storages except for the specified one are inactive, i.e., base.
		# Inactive storages can be used to pull data blocks from, and must
		# be updated on each invocation, since somebody else might be adding
		# blocks there
		#
		self.storages = storages
		self.active_storage_index = active_storage_index

	def get_active_storage_index(self):
		return self.active_storage_index
	
	def get_active_storage(self):
		return self.storages[self.active_storage_index]

	def get_storages(self):
		return self.storages

	def rescan_storage(self,handler):
		# TODO: this should proceed in a separate thread
		# actually, each storage could be processed in its own thread
		for storage_idx in len(self.base_storages):
			if storage_idx == self.active_storage_index:
				continue
			# This is not active storage. Somebody else might be updating it,
			# so rescan
			storage = self.base_storages[storage_idx]
			new_containers = storage.rescan()
			for container_idx in new_containers:
				container = storage.get_container(container_idx)
				#
				# Register blocks of the container in the block_container_db
				#
				container.load_header()
				has_nondata_blocks = False
				has_data_blocks = False
				encoded = self.encode_block_info(storage_idx,container_idx)
				for digest,size,code in container.list_blocks():
					self.block_container_db[digest] = encoded
					if code != Container.CODE_DATA:
						has_nondata_blocks = True
					else:
						has_data_blocks = True
				#
				# Notify the caller of the nondata blocks, which are supposed to be
				# cached
				#
				if has_nondata_blocks:
					container.load_body()
					container.load_blocks(handler)
				if not has_data_blocks:
					container.remove_files()
	def close(self):
		self.block_container_db.close()
		
	def add_block(self,digest,data,code):
		storage = self.get_active_storage()
		#
		# Make sure we have a container that can take this block
		#
		if self.current_open_container is None:
			self.current_open_container = self.storage.open_container()
		if not self.current_open_container.can_add_block(digest,data,code):
			self.current_open_container.finalize()
			#
			# Now we have container idx, update it in the blocks db
			#
			container_idx = self.current_open_container.get_idx()
			encoded = self.encode_block_info(self.active_storage_index,container_idx)
			for digest,code in self.current_open_container.list_blocks():
				self.block_container_db[digest] = encoded
			self.current_open_container = self.storage.open_container()
		#
		# add the block to the container
		#
		self.current_open_container.add_block(digest,data,code)

	def flush(self):
		storage = self.get_active_storage()

		if self.current_open_container is not None:
			storage.finalize_container(self.current_open_container)
			#
			# Now we have container idx, update it in the blocks db
			#
			container_idx = self.current_open_container.get_index()
			encoded = self.encode_block_info(self.active_storage_index,container_idx)
			for digest,code in self.current_open_container.list_blocks():
				self.block_container_db[digest] = encoded
			
	def load_block(self,digest,handler):
		storage_idx,container_idx = self.decode_block_info(self.block_container_db[digest])
		storage = self.storages[storage_idx]
		container = storage.get_container(container_idx)
		container.load_header()
		container.load_body()
		container.load_blocks(handler)
		container.remove_files()

	def get_block_storage(self,digest):
		storage_idx,container_idx = self.decode_block_info(self.block_container_db[digest])
		return storage_idx

	#--------------------------------------------------------
	# Utility methods
	#--------------------------------------------------------
	def encode_block_info(self,storage_idx,container_idx):
		io = StringIO()
		Format.write_int(io,storage_idx)
		Format.write_int(io,container_idx)
		return io.value()
	def decode_block_info(self,encoded):
		io = StringIO(encoded)
		storage_idx = Format.read_int(io)
		container_idx = Format.read_int(io)
		return (storage_idx,container_idx)
