from manent.Nodes import *

class MockContainerConfig:
	def blockSize(self):
		return 32

class MockGlobalConfig:
	def excludes(self):
		return []

class MockIncrement:
	pass

class MockIncrementsDB:
	def __init__(self):
		self.next_index = 0
		self.increments = {}
	def start_increment(self,comment):
		self.increments[self.next_index] = comment
		increment = MockIncrement()
		increment.idx = self.next_index
		self.next_index += 1
	def finalize_increment(self):
		pass
		

class MockBlockCtx:
	def __init__(self,backup):
		self.backup = backup
	def add_block(self,digest,data):
		self.backup.add_block(digest,data)
	def load_block(self,digest):
		return self.backup.load_block(digest)

class MockHlinkCtx:
	def __init__(self):
		self.inodes_db = {}
	
class MockChangeCtx:
	def __init__(self):
		self.total_nodes = 0
		self.changed_nodes = 0
	def get_change_percent(self):
		if self.total_nodes == 0:
			return 0.0
		percent = float(self.changed_nodes)/self.total_nodes
		return percent
		
class MockScanCtx(MockBlockCtx,MockHlinkCtx,MockChangeCtx):
	def __init__(self,backup):
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)
		MockChangeCtx.__init__(self)
	def get_level(self):
		# Assume that backup filled that in
		return self.level

class MockRestoreCtx(MockBlockCtx,MockHlinkCtx):
	def __init__(self,backup):
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)

class MockRepository:
	def __init__(self):
		self.blocks_db = {}
		self.blocks_codes_db = {}
	def load_block(self,digest):
		return self.blocks_db[digest]
	def add_block(self,digest,data,code):
		self.blocks_db[digest] = data
		self.blocks_codes_db[digest] = code
	def block_code(self,digest):
		return self.blocks_codes_db[digest]

class MockBlockDatabase:
	def __init__(self,repository):
		self.repository = repository
	def request_block(self,digest):
		pass
	def add_block(self,digest,data,code):
		self.repository.add_block(digest,data,code)
	def load_block(self,digest):
		return self.repository.load_block(digest)
	def get_block_storage(self,digest):
		pass
	def get_storage_index(self,digest):
		return 0
	def get_block_type(self,digest):
		pass

class MockBackup:
	def __init__(self,home):
		self.container_config = MockContainerConfig()
		self.global_config = MockGlobalConfig()
		self.increments = MockIncrementsDB()
		self.repository = MockRepository()
		self.config_db = {}
		self.home = home
	def blockSize(self):
		return 1024
	
	def start_increment(self,comment):
		increment = self.increments.start_increment(comment)
		ctx = MockScanCtx(self)

		self.ctx = ctx
		self.root_node = Directory(self,None,self.home)
		return ctx
	def finalize_increment(self):
		self.increments.finalize_increment()
	def start_restore(self,idx):
		ctx = MockRestoreCtx(self)
		return ctx
	def is_increment_finalized(self,idx):
		return self.increments.is_increment_finalized(idx)
	
	def add_block(self,digest,data,code):
		self.repository.add_block(digest,data,code)
	def load_block(self,digest):
		return self.repository.load_block(digest)
	def block_code(self,digest):
		return self.repository.block_code(digest)
