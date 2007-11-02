import traceback

import BlockDatabase
import Container
import Database
import IncrementDatabase
import Nodes
import Repository

class Backup:
	"""
	Database of a complete backup set
	"""
	def __init__(self,global_config,label):
		self.global_config = global_config
		self.label = label

		self.db_config = Database.DatabaseConfig(self.global_config,
			"manent.%s.db"%self.label)
		self.txn_handler = Database.TransactionHandler(self.db_config)

	#
	# Three initialization methods:
	# Creation of new Backup, loading from live DB, loading from backups
	#
	def configure(self,data_path):
		#print "Creating backup", self.label, "type:",\
		# container_type, container_params
		self.data_path = data_path
		self.storages = []
		self.active_storage = None

	def add_base_storage(self,storage_type,storage_params):
		pass
	def add_active_storage(self,storage_type,storage_params):
		pass
	
	def remove(self):
		print "Removing backup", self.label
		try:
			self.db_config.remove_database()
		finally:
			self.db_config.close()

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
	def restore(self,target_path):
		try:
			self.__open_all()

			if fs_digest is None:
				fs_digest = self.increments_database.find_last_increment()
			root_node = Nodes.Directory(self,None,target_path)
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
		self.shared_db = self.db_config.get_database(".shared",self.txn_handler)
		self.repository = Repository.Repository(self.db_config,
			storages,active_storage)
		self.blocks_database = BlockDatabase.BlockDatabase(self.db_config,
			self.repository)
		self.increments_database = IncrementDatabase.IncrementDatabase(
			self.repository,self.shared_db)
	
	def __close_all(self):
		self.increments_database.close()
		self.blocks_database.close()
		self.repository.close()
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

	def add_block(self,digest,data,code):
		self.backup.blocks_database.add_block(digest,data,code)

#=========================================================
# RestoreContext
#=========================================================
class RestoreContext:
	def __init__(self):
		self.inodes_db = {}

