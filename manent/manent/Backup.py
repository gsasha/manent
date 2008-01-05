import traceback

import BlockManager
import Container
import Database
import IncrementDatabase
import Nodes
import StorageManager
import ExclusionProcessor

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self, label):
		self.global_config = global_config
		self.label = label

		self.db_config = Database.DatabaseConfig(self.global_config,
			"manent.%s.db"%self.label)
		self.txn_handler = Database.TransactionHandler(self.db_config)
		self.global_config_db = self.db_config.get_database(
			"config.db", "global_config")
		self.private_config_db = self.db_config.get_database(
			"config.db", "config.%s" % self.label)
		
		self.repository = StorageManager.StorageManager(self)
		self.exclusion_processor = ExclusionProcessor.ExclusionProcessor(self)
	#
	# Two initialization methods:
	# Creation of new backup, loading from live DB
	#
	def create(self, data_path):
		assert not self.private_config_db.has_key("data_path")
		self.data_path = data_path
		self.private_config_db["data_path"] = data_path
	def load(self):
		self.data_path = self.private_config_db["data_path"]
		self.repository.load()
		self.exclusion_processor.load()
		
	# Storage configuration
	def add_base_storage(self, storage_params):
		self.repository.add_base_storage(storage_params)
	def add_main_storage(self, storage_params):
		self.repository.add_main_storage(storage_params)

	# Exclusion configuration
	def add_exclusion_rule(self, action, pattern):
		self.exclusion_processor.add_rule(action, pattern)
	
	def remove(self):
		print "Removing backup", self.label
		raise Exception("Not implemented")

	def create(self):
		try:
			self.__open_all()

			# Nothing to do here
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()

	#
	# Scanning (adding a new increment)
	#
	def scan(self,comment):
		try:
			self.__open_all()

			base_fs_digests = self.increments_database.start_increment(comment)
			prev_nums = [(None,None,digest) for digest in base_fs_digests]
			root = Nodes.Directory(self,None,self.data_path)
			root.set_num(ctx.next_number())
			ctx = ScanContext(self,root)
			
			root.scan(ctx,prev_nums)

			print "Diff from previous increments:", ctx.changed_nodes, "out of", ctx.total_nodes
			
			# Upload the special data to the containers
			self.increments_database.finalize_increment(root.get_digest())
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()

	#
	# Restoring an increment to filesystem
	#
	def restore(self, target_path):
		try:
			self.__open_all()

			if fs_digest is None:
				fs_digest = self.increments_database.find_last_increment()
			root_node = Nodes.Directory(self, None, target_path)
			root_node.set_digest(fs_digest)

			ctx = RestoreContext()
			root_node.request_blocks(ctx)
			
			ctx = RestoreContext()
			root_node.restore(ctx)
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()
	
	#
	# Information
	#
	def info(self):
		try:
			self.__open_all()

			# TODO:Print info on all the storages
			# TODO:Print info on all the increments
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()
	
	def __open_all(self):
		self.shared_db = self.db_config.get_database(".shared", self.txn_handler)
		self.storage_manager = StorageManager.StorageManager(self.db_config,
			storages, active_storage)
		self.block_manager = BlockManager.BlockManager(self.db_config,
			self.storage_manager)
		self.increments_database = IncrementDatabase.IncrementDatabase(
			self.storage_manager, self.shared_db)
	
	def __close_all(self):
		self.increments_database.close()
		self.block_manager.close()
		self.storage_manager.close()
		self.shared_db.close()
	
#===============================================================================
# ScanContext
#===============================================================================
class ScanContext:
	def __init__(self,backup,root_node):
		ContextBase.__init__(self,backup)

		self.inodes_db = {}

		self.total_nodes = 0
		self.changed_nodes = 0

		self.root_node = root_node

	def add_block(self, digest, code, data):
		self.backup.block_manager.add_block(digest, code, data)

#=========================================================
# RestoreContext
#=========================================================
class RestoreContext:
	def __init__(self):
		self.inodes_db = {}

