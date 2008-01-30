import base64
import os, os.path
import traceback

import Container
import Database
import IncrementManager
import Nodes
import StorageManager
import ExclusionProcessor

def parse_to_keys(params):
	result = {}
	for kv in params:
		key, value = kv.split('=')
		result[key] = value
	return result

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self, global_config, label):
		self.global_config = global_config
		self.label = label
		self.storage_opened = False

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
		params = parse_to_keys(args[1:])
		if args[0] == 'show':
			for k, v in self.config_db.iteritems():
				print k, '=', v
		elif args[0] == 'set':
			if params.has_key('data_path'):
				self.config_db['data_path'] = params['data_path']
		elif args[0] == 'add_exclusion':
			exclusion_type = params['type']
			exclusion_action = params['action']
			exclusion_pattern = params['pattern']
			if self.config_db.has_key('num_exclusion_rules'):
				n = int(self.config_db['num_exclusion_rules'])
			else:
				n = 0
			if not exclusion_type in ['relative', 'absolute', 'wildcard']:
				raise Exception("Unknown rule type " + exclusion_type)
			if not exclusion_action in ['include', 'exclude']:
				raise Exception("Unknown rule action " + exclusion_action)
			self.config_db['exclusion_rule_%d.type' % n] = exclusion_type
			self.config_db['exclusion_rule_%d.action' % n] = exclusion_action
			self.config_db['exclusion_rule_%d.pattern' % n] = exclusion_pattern
			self.config_db['num_exclusion_rules'] = str(n + 1)
		elif args[0] == 'add_storage':
			self.__open_storage()
			storage_idx = self.storage_manager.add_storage(params, None)
			self.storage_manager.make_active_storage(storage_idx)
		elif args[0] == 'add_base_storage':
			self.__open_storage()
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
			self.__open_storage()

			last_fs_digest, last_fs_level = self.increment_manager.start_increment(comment)
			root = Nodes.Directory(self, None, self.config_db['data_path'])
			ctx = ScanContext(self, root)

			prev_num = (Nodes.NODE_TYPE_DIR, None, last_fs_digest, last_fs_level)
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
	def restore(self, args):
		try:
			self.__open_all()
			self.__open_storage()

			params = parse_to_keys(args)
			storage = int(params['storage'])
			idx = int(params['increment'])
			target = params['target']
			
			increment = self.increment_manager.get_increment(storage, idx)
			root = Nodes.Directory(self, None, target)
			root.set_digest(increment.fs_digest)
			ctx = RestoreContext()
			root.request_blocks(ctx)
			ctx = RestoreContext()
			root.restore(ctx)
			
			self.txn_handler.commit()
		except:
			traceback.print_exc()
			self.txn_handler.abort()
			raise
		finally:
			self.__close_all()
	
	#
	# Testing that an increment can be loaded
	#
	def test(self, args):
		try:
			self.__open_all()
			self.__open_storage()

			params = parse_to_keys(args)
			storage = int(params['storage'])
			idx = int(params['increment'])
			
			increment = self.increment_manager.get_increment(storage, idx)
			root = Nodes.Directory(self, None, "")
			root.set_digest(increment.fs_digest)
			ctx = RestoreContext()
			root.request_blocks(ctx)
			ctx = RestoreContext()
			root.test(ctx)
			
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
	def info(self, args):
		try:
			self.__open_all()
			self.__open_storage()

			detail = args[0]
			params = parse_to_keys(args[1:])

			if detail == 'increments':
				increments = self.increment_manager.get_increments()
				for storage, increment_idxs in increments.iteritems():
					print "Storage", storage, "has increments:", increment_idxs
					for idx in increment_idxs:
						increment = self.increment_manager.get_increment(storage, idx)
						print '  increment', idx, 'comment:', increment.comment,\
							'fs:', base64.b64encode(increment.fs_digest)
			elif detail == 'fs':
				increments = self.increment_manager.get_increments()
				storage = int(params['storage'])
				idx = int(params['increment'])
				increment = self.increment_manager.get_increment(storage, idx)
				print "  increment comment:", increment.comment
				print "  increment fs     :", base64.b64encode(increment.fs_digest)
				print "  increment time   :", increment.ctime
				root = Nodes.Directory(self, None, self.config_db['data_path'])
				root.set_digest(increment.fs_digest)
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
		self.storage_manager.add_block(digest, code, data)
	def load_block(self, digest):
		return self.storage_manager.load_block(digest)
	def request_block(self, digest):
		return self.storage_manager.request_block(digest)
	def get_block_code(self, digest):
		return self.storage_manager.get_block_code(digest)
	def get_completed_nodes_db(self):
		return self.completed_nodes_db

	def __open_all(self):
		self.config_db = self.db_manager.get_database_btree("config.db",
			"data", self.txn_handler)
		self.completed_nodes_db = self.db_manager.get_database("completed_nodes.db",
			"nodes", self.txn_handler)
		self.storage_manager = StorageManager.StorageManager(self.db_manager,
			self.txn_handler)
		#print "DATA PATH", self.config_db['data_path']
		self.exclusion_processor = ExclusionProcessor.ExclusionProcessor(
			self.config_db['data_path'])
		if self.config_db.has_key('num_exclusion_rules'):
			num_exclusion_rules = int(self.config_db['num_exclusion_rules'])
			for r in range(num_exclusion_rules):
				type_str = self.config_db['exclusion_rule_%d.type' % r]
				action_str = self.config_db['exclusion_rule_%d.action' % r]
				pattern = self.config_db['exclusion_rule_%d.pattern' % r]
				if action_str == 'exclude':
					action = ExclusionProcessor.RULE_EXCLUDE
				elif action_str == 'include':
					action = ExclusionProcessor.RULE_INCLUDE
				else:
					raise Exception()
				if type_str == 'absolute':
					self.exclusion_processor.add_absolute_rule(action, pattern)
				elif type_str == 'relative':
					self.exclusion_processor.add_rule(action, pattern)
				elif type_str == 'wildcard':
					self.exclusion_processor.add_wildcard_rule(action, pattern)
	def __open_storage(self):
		# TODO: consider not loading storages on initialization, only on meaningful
		# operations
		self.storage_manager.load_storages(None)
		self.increment_manager = IncrementManager.IncrementManager(
			self.db_manager, self.txn_handler, self.storage_manager)
		self.storage_opened = True
	def __close_all(self):
		if self.storage_opened:
			self.increment_manager.close()
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
