import os, os.path, stat, time
import base64
from cStringIO import StringIO
import re
import manent.utils.IntegerEncodings as IntegerEncodings
import manent.utils.Digest as Digest
import manent.utils.Format as Format
from manent.utils.FileIO import read_blocks
import traceback

import Backup

NODE_TYPE_DIR = 0
NODE_TYPE_FILE = 1
NODE_TYPE_SYMLINK = 2

def node_encode(node_type, node_level=None):
	if node_level == None:
		return node_type
	return node_type + 16 * (node_level+1)
def node_type(node_code):
	return node_code % 16
def node_level(node_code):
	if node_code / 16 == 0:
		return None
	return (node_code / 16)-1
def node_decode(node_code):
	nt = node_type(node_code)
	nl = node_level(node_code)
	return (nt, nl)

#--------------------------------------------------------
# CLASS:Node
#--------------------------------------------------------
class Node:
	"""
	Base class of all the filesystem nodes
	"""
	def __init__(self,backup,parent,name):
		self.backup = backup
		self.parent = parent
		self.name = name

		self.file_path = None
		self.file_stat = None

		self.code = None

	def uncache(self):
		""" For debugging only: uncache all the cached data """
		self.file_path = None
		self.file_stat = None

	def get_type(self):
		return node_type(self.code)
	def get_level(self):
		return node_level(self.code)
	#
	# Path compuations
	#
	def path(self):
		"""
		Compute the full path of the current node
		"""
		if self.file_path is None:
			pathElements = []
			node = self
			while node != None:
				pathElements.append(node.name)
				node = node.parent
			self.file_path = os.path.join(*reversed(pathElements))
		return self.file_path
	def stat(self):
		"""
		Compute the os.stat data for the current file
		"""
		if self.file_stat is None:
			self.file_stat = os.lstat(self.path())
		return self.file_stat
	#
	# Support for scanning hard links
	#
	def scan_hlink(self,ctx):
		file_stat = self.stat()
		nlink = file_stat[stat.ST_NLINK]
		if nlink == 1:
			return False
		inode_num = file_stat[stat.ST_INO]
		if not ctx.inodes_db.has_key(inode_num):
			ctx.inodes_db[inode_num] = (self.number,self.code)
			return False
		# inode found, so reuse it
		(self.number,self.code) = ctx.inodes_db[inode_num]
		return True
	def restore_hlink(self,ctx,stats,restore=True):
		if stats[stat.ST_NLINK] == 1:
			return False
		if not ctx.inodes_db.has_key(self.number):
			ctx.inodes_db[self.number] = self.path()
			return False
		if restore:
			otherFile = ctx.inodes_db[self.number]
			os.link(otherFile, self.path())
		return True
		
	#
	# Support for scanning and restoring operations
	#
	# In all the functions, either level or db_num is defined.
	# - level is defined for a db that is part of the base tree
	# - db_num is defined for an unfinalized db that is part of the
	#   prev list.
	#
	def get_files_db(self,ctx,db_num,level):
		assert (db_num is None and level is not None) \
			or (db_num is not None and level is None)
		
		if level is not None:
			db_num = ctx.base_fs[level]
		return ctx.open_files_dbs[db_num]
	
	def get_stats_db(self,ctx,db_num,level):
		assert (db_num is None and level is not None) \
			or (db_num is not None and level is None)
		
		if level is not None:
			db_num = ctx.base_fs[level]
		return ctx.open_stats_dbs[db_num]
	
	def db_level(self,ctx,db_num,level):
		assert (db_num is None and level is not None) \
			or (db_num is not None and level is None)
		
		if level is not None:
			return level
		return ctx.db_level(db_num)
	
	def db_finalized(self,ctx,db_num,level):
		assert (db_num is None and level is not None) \
			or (db_num is not None and level is None)
		
		if level is not None:
			return True
		return ctx.is_finalized(db_num)
	#
	# Support for scanning in previous increments
	#
	def scan_prev(self,ctx,prev_nums):
		"""
		Search the node in the upper levels of the tree.

		The input, prev_nums, contains a list of the bases, plus probably
		one last unfinalized scan (which can exist if the previous scan was
		terminated in the middle).
		
		If the node is found, then self.code and self.num are set to the
		correct values, and True is returned. Otherwise, False is returned.
		"""
		ctx.total_nodes += 1
		# For checking an assertion
		DEBUG_finalized_seen = False
		
		for (prev_db_num,prev_file_num,prev_file_code) in reversed(prev_nums):
			#
			# Get the databases corresponding to this prev node
			#
			(prev_node_code,prev_node_level) = node_decode(prev_file_code)
			prev_db_finalized = self.db_finalized(ctx,prev_db_num,prev_node_level)
			prev_db_level     = self.db_level    (ctx,prev_db_num,prev_node_level)
			prev_files_db     = self.get_files_db(ctx,prev_db_num,prev_node_level)
			prev_stats_db     = self.get_stats_db(ctx,prev_db_num,prev_node_level)

			#
			# After we have seen one finalized db, the rest must be finalized too
			#
			if DEBUG_finalized_seen:
				assert prev_db_finalized
			else:
				DEBUG_finalized_seen = prev_db_finalized

			#
			# Load the old data
			#
			prev_key = self.node_key(prev_file_num)
			if not prev_stats_db.has_key(prev_key):
				# key is not there, but it can be in an earlier (unfinalized) db
				continue
				
			#print "Success to load", old_key, "from", db_num, ":", file_num, ":", file_code
			prev_stat_data = prev_stats_db[prev_key]
			prev_file_data = prev_files_db[prev_key]
			
			if not prev_db_finalized:
				#
				# Whether we reuse the old data or not, it is not necessary
				# anymore:
				# - if we reuse it, we copy the data into the new files db
				# - if we don't, then it has changed and thus no longer relevant
				#
				del prev_stats_db[prev_key]
				del prev_files_db[prev_key]

			#
			# Compare the contents of the old database to see if it is
			# reusable
			#
			prev_stat = Format.deserialize_ints(prev_stat_data)
			file_stat = self.stat()
			if stat.S_IFMT(file_stat[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
				#print "  Node type differs"
				break
			if file_stat[stat.ST_INO] != prev_stat[stat.ST_INO]:
				#print "  Inode number differs: was %d, now %d" % (file_stat[stat.ST_INO],old_stat[stat.ST_INO]), file_stat
				break
			if file_stat[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
				#print "  Mtime differs"
				break
			if time.time() - file_stat[stat.ST_MTIME] <= 1.0:
				# The time from the last change is less than the resolution
				# of time() functions
				#print "  File too recent",file_stat[stat.ST_MTIME],time.time()
				break
			
			#
			# OK, the old node seems to be the same as this one.
			# Reuse it.
			#
			if prev_node_level is not None:
				# The reference node is already a based one.
				# Don't write data to current DB, instead, the parent dir
				# will write a reference to the base file number
				assert prev_db_finalized
				self.number = prev_file_num
				self.code = prev_file_code
			elif prev_db_level is not None:
				# The reference node is found in the last finalized DB,
				# and that DB is nominated to be a base itself.
				assert prev_db_finalized
				self.number = prev_file_num
				self.code = node_encode(prev_node_code,prev_db_level)
			else:
				# File contents in the previous increment are different from
				# those of the base (and thus the file is not of FILE_BASED type).
				# In this case, we still add the file to current DB, and count it
				# as changed, to make sure next time we'll probably start an unbased
				# increment
				assert not prev_db_finalized
				ctx.changed_nodes += 1
				key = self.get_key()
				ctx.new_files_db[key] = prev_file_data
				ctx.new_stats_db[key] = prev_stat_data
				
				self.code = node_encode(self.node_code())
			return True
		#
		# None of the previous nodes matched.
		# This node must be a new one.
		#
		ctx.changed_nodes += 1
		return False
	#
	# Node configuration
	#
	def node_key(self,num):
		return IntegerEncodings.binary_encode_int_varlen(num)

	#
	# Node serialization to db
	#
	def flush(self,ctx):
		pass

	def set_num(self,num):
		self.number = num
	def get_key(self):
		return self.node_key(self.number)

#--------------------------------------------------------
# CLASS:File
#--------------------------------------------------------
class File(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def node_code(self):
		return NODE_TYPE_FILE
	#
	# Scanning and restoring
	#
	def scan(self,ctx,prev_nums):
		if not stat.S_ISREG(self.stat()[stat.ST_MODE]):
			# TODO: this is strange - a file can change to become a link
			# or a dir from a regular file
			raise Exception("File %s does not contain a regular file"%self.path())

		#
		# Check if we have encountered this file during this scan already
		#
		if self.scan_hlink(ctx):
			return

		#
		# Check if the file is the same as in one of the upper levels
		#
		if self.scan_prev(ctx,prev_nums):
			return
		
		# --- File not yet in database, process it
		valueS = StringIO()
		
		for data in read_blocks(open(self.path(), "rb"), self.backup.container_config.blockSize()):
			digest = Digest.dataDigest(data)
			ctx.add_block(data,digest)
			valueS.write(digest)
			
		# --- Serialize to the filesystem db
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())

		self.code = node_encode(self.node_code())
		
	def restore(self,ctx,base_level):
		"""
		Recreate the data from the information stored in the
		backup
		"""
		db_num = ctx.db_num
		files_db = self.get_files_db(ctx,ctx.db_num,base_level)
		stats_db = self.get_stats_db(ctx,ctx.db_num,base_level)
		key = self.get_key()

		#
		# Check if the file has already been processed
		# during this pass
		#
		stats = Format.unserialize_ints(stats_db[key])
		if self.restore_hlink(ctx,stats):
			return

		#
		# No, this file is new. Create it.
		#
		print "Restoring file", self.path()
		valueS = StringIO(files_db[key])
		file = open(self.path(), "wb")
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(ctx.blocks_cache.load_block(digest))
		#
		# TODO: restore file permissions and other data
		#
	
	def request_blocks(self,ctx,base_level):
		"""
		Put requests for the blocks of the file into the blocks cache
		"""
		files_db = self.get_files_db(ctx,ctx.db_num,base_level)
		stats_db = self.get_stats_db(ctx,ctx.db_num,base_level)
		key = self.get_key()
		stats = Format.unserialize_ints(stats_db[key])

		#
		# Check if the file has already been processed
		# during this pass
		#
		if self.restore_hlink(ctx,stats,restore=False):
			return
		
		#
		# Ok, this file is new. Count all its blocks
		#
		valueS = StringIO(db[key])
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			ctx.blocks_cache.request_block(digest)
	def list_files(self,ctx,base_level):
		(node_code,node_level) = node_decode(self.code)
		if node_level is not None:
			print "B%d" % node_level,
		elif base_level is not None:
			print "B%d" % base_level,
		print self.path(), self.number

#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def node_code(self):
		return NODE_TYPE_SYMLINK
	def scan(self,ctx,prev_nums):
		print "scanning symlink", self.path(), prev_nums
		if not stat.S_ISLNK(self.stat()[stat.ST_MODE]):
			raise Exception("File %s does not contain a symlink file"%self.path())
		
		if self.scan_hlink(ctx):
			return

		if self.scan_prev(ctx,prev_nums):
			return

		self.link = os.readlink(self.path())

		key = self.get_key()
		ctx.new_files_db[key] = self.link
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())

		self.code = node_encode(self.node_code())
		
	def restore(self,ctx,base_level):
		print "Restoring symlink in", self.path()
		db_num = ctx.db_num
		files_db = self.get_files_db(ctx,ctx.db_num,base_level)
		stats_db = self.get_stats_db(ctx,ctx.db_num,base_level)
		
		key = self.get_key()
		valueS = StringIO(db[key])
		stats = Format.unserialize_ints(stats_db[key])
		if self.restore_hlink(ctx,stats):
			return
		
		self.link = db[key]
		print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
	def list_files(self,ctx,based):
		(node_code,node_level) = node_decode(self.code)
		if node_level is not None:
			print "B%d" % node_level,
		elif base_level is not None:
			print "B%d" % base_level,
		print self.path(), self.number, self.link

##--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def node_code(self):
		return NODE_TYPE_DIR
	def read_directory_entries(file):
		node_type = valueS.read(1)
		if len(node_type) == 0:
			raise StopIteration
		node_num = Format.read_int(file)
		node_name = Format.read_string(file)
		yield (node_type,node_num,node_name)
	def scan(self,ctx,prev_nums):
		if not stat.S_ISDIR(self.stat()[stat.ST_MODE]):
			raise Exception("File %s does not contain a regular file"%self.path())
		#
		# Reload data from previous increments.
		#
		print "Path", self.path(), "found in previous increments:", prev_nums
		# prev_data keeps for each file in this directory the list of backups
		# in which it was found
		prev_data = {}
		self.base_nodes = None
		for (db_num,file_num,file_code) in reversed(prev_nums):
			#
			# Get the data from the reference database, and delete it
			# from there if possible.
			#
			(node_code, node_level) = node_decode(file_code)
			old_db_finalized = self.db_finalized(ctx,db_num,node_level)
			old_db_level = self.db_level(ctx,db_num,node_level)
			old_files_db = self.get_files_db(ctx,db_num,node_level)
			old_stats_db = self.get_stats_db(ctx,db_num,node_level)

			old_key = self.node_key(file_num)
			if not files_db.has_key(old_key):
				print "DB %d doesn't have key [%s]"  % (db_num,base64.b64encode(old_key))
				continue

			old_stat_data = old_stats_db[old_key]
			old_file_data = old_files_db[old_key]
			if not old_db_finalized:
				del old_stats_db[old_key]
				del old_files_db[old_key]
			#
			# Check if this dir is a possible base for us
			#
			if base_nodes is None and node_level is not None:
				base_number = file_num
				base_code = file_code
				self.base_nodes = {}
			elif base_nodes is None and old_db_level is not None:
				assert old_db_finalized
				base_number = file_num
				base_code = node_encode(node_code,old_db_level)
				self.base_nodes = {}
			else:
				base_number = None
				base_code = None
			#
			# Parse the data from the reference database
			#
			valueS = StringIO(old_file_data)
			for (node_type,node_num,node_name) in read_directory_entries(valueS):
				if prev_data.has_key(node_name):
					# We are interested in only the latest scan of this node
					continue
				prev_data[node_name] = (db_num,file_num,file_code)
				if self.base_nodes is not None:
					self.base_nodes[node_name] = 1
		#print "Prev data:"
		#keys = prev_data.keys()
		#keys.sort()
		#for key in keys:
			#print " ", key, prev_data[key]
		#print "base data:"
		#keys = base_nodes.keys()
		#keys.sort()
		#for key in keys:
			#print " ", key, base_nodes[key]
		#
		# Scan the directory
		#
		# TODO: there shouldn't be two lists
		print "starting scan for", self.path()
		self.modified = True
		self.same_as_base = True
		# This will be overridden when we finish scanning and see
		# that nothing changed
		self.code = NODE_DIR
		self.children = []
		if self.path().startswith(self.backup.global_config.home_area()):
			entries = []
		else:
			entries = os.listdir(self.path())
		for name in entries:
			if (file=="..") or (file=="."):
				continue
			path = os.path.join(self.path(),name)
			file_mode = os.lstat(path)[stat.ST_MODE]
			cur_prev_nums = []
			if prev_data.has_key(name):
				cur_prev_nums = prev_data[name]
			try:
				self.modified = True
				if stat.S_ISLNK(file_mode):
					node = Symlink(self.backup,self, name)
					node.set_num(ctx.next_num())
					node.scan(ctx,cur_prev_nums)
					self.children.append(node)
					if node.get_level() == None:
						self.same_as_base = False
					else:
						del base_nodes[name]
				elif stat.S_ISREG(file_mode):
					node = File(self.backup,self,name)
					node.set_num(ctx.next_num())
					same = node.scan(ctx,cur_prev_nums)
					self.children.append(node)
					if node.get_level() == None:
						self.same_as_base = False
					elif node.code == NODE_FILE_BASED:
						del base_nodes[name]
				elif stat.S_ISDIR(file_mode):
					node = Directory(self.backup,self,name)
					node.set_num(ctx.next_num())
					# The order of append and scan is different here!
					self.children.append(node)
					same = node.scan(ctx,cur_prev_nums)
					if node.get_level() == None:
						self.same_as_base = False
					elif node.code == NODE_DIR_BASED:
						del base_nodes[name]
				else:
					print "Ignoring unrecognized file", name
					same = True
			except OSError:
				print "OSError accessing", path
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno,strerror), path
				traceback.print_exc()

		if self.same_as_base == True and len(base_nodes) != 0:
			print "Files remained in directory, so it's not the same"
			self.same_as_base = False

		if (self.number != 0 and
			self.base_nodes is not Null and len(self.base_nodes) == 0 and
			self.same_as_base == True):
			self.number = base_number
			self.code = base_code
		else:
			self.code = node_encode(NODE_TYPE_DIR)
			self.flush(ctx)

		# remove the children list - we don't want to keep everything in memory
		# while scanning
		self.children = None
	def flush(self,ctx):
		"""
		Flush the contents of the current node.
		Called when a container is completed or when
		the node is completed
		"""
		if self.children == None:
			# Node has finished and flushed. Nothing more to do
			return
		
		for child in self.children:
			child.flush(ctx)
		
		if not self.modified:
			# Nothing has happened since last flush
			return
		if self.base_nodes is not None and self.same_as_base:
			# So far, we have seen nothing to suggest that this
			# dir will be different from the base one.
			# Therefore, the base is good for re-scanning, no need
			# to create the node yet.
			return
		
		valueS = StringIO()
		for child in self.children:
			valueS.write(child.code)
			
			Format.write_int(valueS,child.number)
			Format.write_string(valueS,child.name)
			
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())
		self.modified = False
		#print "Flushing node", self.path(), "to", base64.b64encode(key)
		
	def restore(self,ctx,based=False):
		if self.parent != None:
			print "Restoring dir", self.path(), self.number
			os.mkdir(self.path())

		db_num = ctx.db_num
		files_db = self.get_files_db(ctx,ctx.db_num,base_level)
		stats_db = self.get_stats_db(ctx,ctx.db_num,base_level)
		key = self.get_key()
		valueS = StringIO(files_db[key])
		for (node_code,node_num,node_name) in read_directory_entries(valueS):
			file_type = node_type(node_code)
			if file_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self, node_name)
			elif file_type == NODE_TYPE_FILE:
				node = File(self.backup,self,node_name)
			elif file_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup,self,node_name)
			else:
				raise "Unknown node type [%s]"%node_type

			node.set_num(node_num)
			node.code = node_type
			node.restore(ctx,based)

	def request_blocks(self,ctx,block_cache,based=False):
		#print "Requesting blocks in", self.path(), "code", self.code, "num", self.number, "based", based
		db_num = ctx.db_num
		files_db = self.get_files_db(ctx,ctx.db_num,base_level)
		#stats_db = self.get_stats_db(ctx,ctx.db_num,base_level)
		key = self.get_key()
		valueS = StringIO(files_db[key])
		for (node_code,node_num,node_name) in read_directory_entries(valueS):
			file_type = node_type(node_code)
			if file_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self,node_name)
			elif file_type == NODE_TYPE_FILE:
				node = File(self.backup,self,node_name)
			elif file_type == NODE_TYPE_SYMLINK:
				# Nothing to do for symlinks
				continue
			else:
				raise "Unknown node type [%s]"%node_type
			node.set_num(node_num)
			node.code = node_type
			node.request_blocks(ctx,block_cache,based)

	def list_files(self,ctx,based=False):
		files_db = self.get_files_db(self.code, default_db = ctx.files_db)
		stats_db = self.get_stats_db(self.code, default_db = ctx.stats_db)
			
		print self.path(), self.number
		key = self.get_key()
		valueS = StringIO(files_db[key])
		for (node_code,node_num,node_name) in read_directory_entries(valueS):
			file_type = node_type(node_code)
			if file_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self,node_name)
			elif file_type == NODE_TYPE_FILE:
				node = File(self.backup,self,node_name)
			elif file_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup,self,node_name)
			node.set_num(node_num)
			node.code = node_type
			#print "Child node", node_name, "code:", node_type
			node.list_files(ctx,based)
