import os, os.path
import traceback

import BlockManager
import Container
import Database
import IncrementManager
import Nodes
import StorageManager
import ExclusionProcessor

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self, global_config, label):
		self.global_config = global_config
		self.label = label

		try:
			os.makedirs(os.path.join(global_config.home_area(), self.label))
		except:
			# It's OK to fail, if the directory already exists
			pass
		try:
			os.makedirs(os.path.join(global_config.staging_area(), self.label))
		except:
			# It's OK to fail, if the directory already exists
			pass
		self.db_manager = Database.DatabaseManager(self.global_config,
			self.label)
		self.txn_handler = Database.TransactionHandler(self.db_manager)
	#
	# Two initialization methods:
	# Creation of new backup, loading from live DB
	#
	def configure(self, args):
		self.__open_all()
		params = {}
		for kv in args[1:]:
			key, value = kv.split("=")
			params[key] = value
		if args[0] == 'show':
			for k, v in self.config_db.iteritems():
				print k, '=', v
		elif args[0] == 'set':
			if params.has_key('data_path'):
				self.config_db['data_path'] = params['data_path']
		elif args[0] == 'exclusions':
			pass
		elif args[0] == 'add_storage':
			storage_idx = self.storage_manager.add_storage(params, None)
			self.storage_manager.make_active_storage(storage_idx)
		elif args[0] == 'add_base_storage':
			self.storage_manager.add_storage(params, None)
		self.txn_handler.commit()

	def load(self):
		self.data_path = self.config_db["data_path"]
		self.storage_manager.load()
		self.exclusion_processor.load()
	
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
	def scan(self, comment):
		try:
			self.__open_all()

			last_fs_digest = self.increment_manager.start_increment(comment)
			root = Nodes.Directory(self, None, self.config_db['data_path'])
			ctx = ScanContext(self, root)

			prev_num = (Nodes.NODE_TYPE_DIR, None, last_fs_digest)
			root.scan(ctx, prev_num, self.exclusion_processor)

			print "Diff from previous increments:", ctx.changed_nodes, "out of", ctx.total_nodes
			
			# Upload the special data to the containers
			self.increment_manager.finalize_increment(root.get_digest())
			self.storage_manager.flush()
			
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
				fs_digest = self.increment_manager.find_last_increment()
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

			root = Nodes.Directory(self, None, self.config_db['data_path'])
			root.list_files()
			# TODO:Print info on all the storages
			# TODO:Print info on all the increments
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()
	
	def get_block_size(self):
		return self.storage_manager.get_block_size()
	def add_block(self, digest, code, data):
		self.block_manager.add_block(digest, code, data)
	def load_block(self, digest):
		return self.block_manager.load_block(digest)
	def get_block_code(self, digest):
		return self.block_manager.get_block_code(digest)
	def get_completed_nodes_db(self):
		return self.completed_nodes_db

	def __open_all(self):
		self.config_db = self.db_manager.get_database_btree("config.db",
			"data", self.txn_handler)
		self.completed_nodes_db = self.db_manager.get_database("completed_nodes.db",
			"nodes", self.txn_handler)
		self.storage_manager = StorageManager.StorageManager(self.db_manager,
			self.txn_handler)
		# TODO: consider not loading storages on initialization, only on meaningful
		# operations
		self.storage_manager.load_storages(None)
		self.block_manager = BlockManager.BlockManager(self.db_manager,
			self.txn_handler, self.storage_manager)
		self.increment_manager = IncrementManager.IncrementManager(
			self.db_manager, self.txn_handler, self.block_manager,
			self.storage_manager)
		print "DATA PATH", self.config_db['data_path']
		self.exclusion_processor = ExclusionProcessor.ExclusionProcessor(
			self.config_db['data_path'])

	def __close_all(self):
		self.increment_manager.close()
		self.block_manager.close()
		self.storage_manager.close()
		self.completed_nodes_db.close()
		self.config_db.close()
	
#===============================================================================
# ScanContext
#===============================================================================
class ScanContext:
	def __init__(self, backup, root_node):

		self.inodes_db = {}

		self.total_nodes = 0
		self.changed_nodes = 0

		self.root_node = root_node

#=========================================================
# RestoreContext
#=========================================================
class RestoreContext:
	def __init__(self):
		self.inodes_db = {}

