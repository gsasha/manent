from manent.Nodes import *

class MockContainerConfig:
	def blockSize(self):
		return 32

class MockGlobalConfig:
	def excludes(self):
		return []

class MockIncrementFSCtx:
	def __init__(self,backup):
		self.backup = backup
	#
	# Funcionality for scanning
	#
	def get_db_level(self,idx):
		# always say that the new db is not a new base
		return self.backup.get_db_level(idx)
	def is_db_base(self,idx):
		return idx in self.bases
	def is_db_finalized(self,idx):
		return self.backup.is_increment_finalized(idx)
	def get_files_db(self,idx):
		return self.backup.files_db[idx]
	def get_stats_db(self,idx):
		return self.backup.stats_db[idx]

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
	
class MockNumberCtx:
	def __init__(self):
		self.current_num = 0
	def next_number(self):
		self.current_num += 1
		return self.current_num

class MockChangeCtx:
	def __init__(self):
		self.total_nodes = 0
		self.changed_nodes = 0
	def get_change_percent(self):
		if self.total_nodes == 0:
			return 0.0
		percent = float(self.changed_nodes)/self.total_nodes
		return percent
		
class MockScanCtx(MockIncrementFSCtx,MockBlockCtx,MockHlinkCtx,MockNumberCtx,MockChangeCtx):
	def __init__(self,backup):
		MockIncrementFSCtx.__init__(self,backup)
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)
		MockNumberCtx.__init__(self)
		MockChangeCtx.__init__(self)
	def get_level(self):
		# Assume that backup filled that in
		return self.level

class MockRestoreCtx(MockIncrementFSCtx,MockBlockCtx,MockHlinkCtx):
	def __init__(self,backup):
		MockIncrementFSCtx.__init__(self,backup)
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)

class MockBackup:
	def __init__(self,home):
		self.container_config = MockContainerConfig()
		self.global_config = MockGlobalConfig()
		self.config_db = {}
		self.blocks_db = {}
		self.home = home
	def start_increment(self,comment):
		increment = self.increments.start_increment(comment)
		ctx = MockScanCtx(self)
		ctx.new_files_db = {}
		ctx.new_stats_db = {}
		self.files_db[increment.idx] = ctx.new_files_db
		self.stats_db[increment.idx] = ctx.new_stats_db
		ctx.bases = increment.bases
		ctx.scan_bases = increment.scan_bases
		ctx.level = self.get_db_level(increment.idx)

		self.ctx = ctx
		self.root_node = Directory(self,None,self.home)
		return ctx
	def finalize_increment(self,percent_change):
		class Handler:
			def __init__(self,backup):
				self.backup = backup
			def remove_increment(self,idx):
				del self.backup.files_db[idx]
				del self.backup.stats_db[idx]
			def rebase_fs(self,bases):
				print "rebasing last increment to ", bases
				ctx = MockChangeCtx()
				self.root_node.rebase(ctx,bases)
		self.increments.finalize_increment(percent_change,Handler(self))
	def start_restore(self,idx):
		ctx = MockRestoreCtx(self)
		return ctx
	def is_increment_finalized(self,idx):
		return self.increments.is_increment_finalized(idx)
	def add_block(self,digest,data):
		self.blocks_db[digest] = data
	def load_block(self,digest):
		return self.blocks_db[digest]
	def get_db_level(self,idx):
		return self.increments.get_level(idx)
