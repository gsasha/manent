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

#----------------------------------------------------
# Node conversion
#----------------------------------------------------
NODE_TYPE_DIR     = 0
NODE_TYPE_FILE    = 1
NODE_TYPE_SYMLINK = 2
NODE_TYPE_MAX     = 8

def node_encode(node_type, node_level):
	return node_type + NODE_TYPE_MAX * (node_level)
def node_type(node_code):
	return node_code % NODE_TYPE_MAX
def node_level(node_code):
	return node_code / NODE_TYPE_MAX
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

		self.cached_path = None
		self.cached_stat = None

		self.code = None

	def uncache(self):
		""" For debugging only: uncache all the cached data """
		self.cached_path = None
		self.cached_stat = None

	def get_type(self):
		# Should be overridden by derived classes
		return None
	def get_level(self):
		return self.level
	def set_level(self,level):
		self.level = level
	def get_number(self):
		return self.number
	def set_number(self,number):
		self.number = number
	#
	# Path compuations
	#
	def path(self):
		"""
		Compute the full path of the current node
		"""
		if self.cached_path is None:
			pathElements = []
			node = self
			while node != None:
				pathElements.append(node.name)
				node = node.parent
			self.cached_path = os.path.join(*reversed(pathElements))
		return self.cached_path
	def stat(self):
		"""
		Compute the os.stat data for the current file
		"""
		if self.cached_stat is None:
			self.cached_stat = os.lstat(self.path())
		return self.cached_stat
	#-----------------------------------------------------
	# Support for scanning hard links:
	# 
	# see if the current file is a hard link to another file
	# that has already been scanned. If so, reuse it.
	#
	def scan_hlink(self,ctx):
		file_stat = self.stat()
		nlink = file_stat[stat.ST_NLINK]
		if nlink == 1:
			return False
		inode_num = file_stat[stat.ST_INO]
		if not ctx.inodes_db.has_key(inode_num):
			ctx.inodes_db[inode_num] = (self.code,self.number)
			return False
		# inode found, so reuse it
		(self.code,self.number) = ctx.inodes_db[inode_num]
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
	# Support for scanning in previous increments
	#
	def scan_prev(self,ctx,prev_nums):
		"""
		Search the node in the upper levels of the tree.

		The input, prev_nums, contains a list of the bases, plus the
		later scans (which can be either finalized or unfinalized).
		
		If the node is found, then self.code and self.num are set to the
		correct values, and True is returned. Otherwise, False is returned.
		"""
		ctx.total_nodes += 1
		
		for (prev_idx,prev_type,prev_number) in reversed(prev_nums):
			#
			# Get the databases corresponding to this prev node
			#
			prev_files_db = ctx.get_files_db(prev_idx)
			prev_stats_db = ctx.get_stats_db(prev_idx)

			#
			# Load the old data
			#
			prev_key = self.compute_key(prev_number)
			if not prev_stats_db.has_key(prev_key):
				# key is not there, but it can still be in an earlier db
				continue
				
			#
			# Compare the contents of the old database to see if it is
			# reusable
			#
			prev_stat = Format.deserialize_ints(prev_stats_db[prev_key])
			cur_stat = self.stat()
			if stat.S_IFMT(cur_stat[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
				#print "  Node type differs"
				break
			if cur_stat[stat.ST_INO] != prev_stat[stat.ST_INO]:
				#print "  Inode number differs: was %d, now %d" % (file_stat[stat.ST_INO],old_stat[stat.ST_INO]), file_stat
				break
			if cur_stat[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
				#print "  Mtime differs"
				break
			if time.time() - file_stat[stat.ST_MTIME] <= 1.0:
				# The time from the last change is less than the resolution
				# of time() functions
				#print "  File too recent",file_stat[stat.ST_MTIME],time.time()
				break
			
			#
			# OK, the prev node seems to be the same as this one.
			# Reuse it.
			#
			if ctx.is_base_level(prev_idx):
				# The reference node is already a based one.
				# Don't write data to current DB, instead, the parent dir
				# will write a reference to the base file number
				self.level = ctx.get_level(prev_idx)
				self.number = prev_number
				return True
		
		ctx.changed_nodes += 1
		return False
	def restore_stats(self,stats):
		prev_stat = os.lstat(self.path())
		os.chmod(self.path(),stats[stat.ST_MODE])
		os.chown(self.path(),stats[stat.ST_UID],stats[stat.ST_GID])
		os.utime(self.path(),(stats[stat.ST_ATIME],stats[stat.ST_MTIME]))
	#
	# Node configuration
	#
	def compute_key(self,num):
		return IntegerEncodings.binary_encode_int_varlen(num)

	def get_key(self):
		return self.compute_key(self.number)

#--------------------------------------------------------
# CLASS:File
#
# Semantics of level:
# There are two numbers that identify a db in the ctx: idx and level
# - the database corresponding to them is the same one for all idxs that are
#   bases.
# - for dbs that are not bases, the allows access of the db from ctx, and level is invalid.
#--------------------------------------------------------
class File(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def get_type(self):
		return NODE_TYPE_FILE
	#
	# Scanning and restoring
	#
	def scan(self,ctx,prev_nums):
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
			ctx.add_block(digest,data)
			valueS.write(digest)
			
		# --- Serialize to the filesystem db
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())

		self.code = node_encode(self.get_type(), self.get_level())
		
	def restore(self,ctx):
		"""
		Recreate the data from the information stored in the
		backup
		"""
		files_db = ctx.get_files_db(self.get_level())
		stats_db = ctx.get_stats_db(self.get_level())
		key = self.get_key()

		#
		# Check if the file has already been processed
		# during this pass
		#
		stats = Format.deserialize_ints(stats_db[key])
		if self.restore_hlink(ctx,stats):
			return

		#
		# No, this file is new. Create it.
		#
		#print "Restoring file", self.path()
		valueS = StringIO(files_db[key])
		file = open(self.path(), "wb")
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(ctx.load_block(digest))
		file.close()
		self.restore_stats(stats)
	
	def request_blocks(self,ctx):
		"""
		Put requests for the blocks of the file into the blocks cache
		"""
		files_db = ctx.get_files_db(self.get_level())
		stats_db = ctx.get_stats_db(self.get_level())
		key = self.get_key()
		stats = Format.deserialize_ints(stats_db[key])

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
			ctx.request_block(digest)
	def list_files(self,ctx):
		(node_code,node_level) = node_decode(self.code)
		print "B%d" % node_level, self.path(), self.number

#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def get_type(self):
		return NODE_TYPE_SYMLINK
	def scan(self,ctx,prev_nums):
		print "scanning symlink", self.path(), prev_nums
		
		if self.scan_hlink(ctx):
			return

		if self.scan_prev(ctx,prev_nums):
			return

		self.link = os.readlink(self.path())

		key = self.get_key()
		ctx.new_files_db[key] = self.link
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())

		self.code = node_encode(self.get_type(),self.get_level())
		
	def restore(self,ctx):
		#print "Restoring symlink in", self.path()
		files_db = self.get_files_db(ctx,self.get_level())
		stats_db = self.get_stats_db(ctx,self.get_level())
		
		key = self.get_key()
		valueS = StringIO(db[key])
		stats = Format.deserialize_ints(stats_db[key])
		if self.restore_hlink(ctx,stats):
			return
		
		self.link = db[key]
		#print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
		self.restore_stats(stats)
	def list_files(self,ctx):
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
	def get_type(self):
		return NODE_TYPE_DIR
	def read_directory_entries(file):
		node_code = Format.read_int(file)
		if node_code is None:
			raise StopIteration
		node_num = Format.read_int(file)
		node_name = Format.read_string(file)
		yield (node_code,node_num,node_name)
	def scan(self,ctx,prev_nums):
		"""
		Scan the node, considering data in all the previous increments

		algorithm:
		1. Scan the previous nodes. Prepare the following data structure "prev_data":
		  - for every file name in the previous database nums, keep the list of:
		    1. database idx
		    2. file number
		  Prepare the following data structure "prev_dir_data":
		  - for every base directory level, keep the map of name->(level,code)
		2. Scan the files in the directory. For every file, check if it is in prev_data:
		  1. if it is the same as in prev_data[-1], use it as a base or as a copy.
		  2. if it is not the same or not found in prev_data, scan the file recursively
		     and note that a change is taking place.
		  3. During the scan, compute serialization of the current info.
		     - If there is no change, do not write the serialization
			 - If a change was noted, do the writing using exponential falloff
		3. After the scan has completed:
		  1. If the last level is a based one...
		  TODO!!!
		"""
		#
		# Reload data from previous increments.
		#
		print "Path", self.path(), "found in previous increments:", prev_nums
		#
		# Get the data from the reference databases, and delete it
		# from there if possible.
		#
		# prev data indexed by file, for directory scan
		prev_name_data = {}
		# prev data indexed by level, for search of base dir
		prev_level_data = {}
		prev_level_file_info = {}
		
		for (prev_idx,prev_type,prev_number) in prev_nums:
			#
			# Get the databases corresponding to this prev node
			#
			if prev_type != self.get_type():
				# The same file name previously was of another type.
				# It definitely can't serve as a base
				continue
			
			prev_files_db = self.ctx.get_files_db(prev_idx)
			prev_stats_db = self.ctx.get_stats_db(prev_idx)

			prev_key = self.compute_key(prev_number)
			if not prev_files_db.has_key(prev_key):
				print "Prev DB %d doesn't have key [%s]"  % (prev_idx,base64.b64encode(prev_key))
				continue

			prev_stat_data = prev_stats_db[prev_key]
			prev_file_data = prev_files_db[prev_key]
			# TODO!!! Since we write into db only where we see difference, it's OK
			#      to delete unfinalized dbs only when one is finalized. Or not!

			#
			# Compare the filesystem contents to the old database to see if it is
			# reusable
			#
			prev_stat = Format.deserialize_ints(prev_stat_data)
			if stat.S_IFMT(self.stat()[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
				#print "  Node type differs"
				# Previous node is not a directory, nothing to do with it.
				break
			# Do not check inode and acces time like in scan_prev,
			# as directory can be a base even when it has been changed somewhat
			
			#
			# Parse the base directory data
			#
			if ctx.is_base_db(prev_idx):
				# prev_level_data is computed only for base dbs
				assert prev_idx == ctx.get_level(prev_idx)
				prev_level_file_info[prev_level] = (prev_idx,prev_num)
				prev_level_data[prev_level] = {}

			valueS = StringIO(prev_file_data)
			for (code,number,name) in read_directory_entries(valueS):
				#---------------------------------------
				# Process construction of prev_name_data
				#---------------------------------------
				if not prev_name_data.has_key(name):
					prev_name_data[name] = []
				the_type, level = node_decode(code)
				# If the file listed in the directory is based, its index is same as
				# its level. Otherwise, its index is same as the directory index
				if ctx.get_node_level(prev_idx) > level:
					idx = level
				else:
					idx = prev_idx
				# It is possible for several directories to point to the same version
				# of a linked file. In this case, store it only once in the prev information
				if not (idx,the_type,number) in prev_name_data[name]:
					prev_name_data[name].append((prev_idx,the_type,number))
				#----------------------------------------
				# Process construction of prev_level_data
				#----------------------------------------
				# prev_level_data is collected only for dbs that are bases
				if ctx.is_base_db(prev_idx):
					# Note that here we use the level for comparison, since this is actual
					# information read from the db
					prev_level_data[prev_base_level][name] = (level,the_type,number)

		#
		# Initialize scanning data
		#
		self.modified = False
		self.dirty = False
		self.cur_data = {}
		# The list of children is used for flushing
		self.subdirs = []

		#
		# Check if this directory must be excluded.
		# If so, operate as if it's empty.
		# TODO: exclude individual files too!
		#
		# The following is not necessary since the directory should be excluded as a file
		#for checker in self.backup.global_config.excludes():
			#if checker(self.path()):
				#entries = []
				#print "Excluding directory from scan:", self.path()
				#break
		#else:
			#entries = os.listdir(self.path())
		
		#
		# Scan the directory
		#
		print "starting scan for", self.path()
		for name in entries:
			# Exclude nonessential names automatically
			if (name=="..") or (name=="."):
				continue
			
			path = os.path.join(self.path(),name)
			
			# Check if the file name should be excluded
			excluded = False
			for checker in self.backup.global_config.excludes():
				if checker(path):
					print "Excluding file from scan:", path
					excluded = True
					break
			if excluded: continue
			
			file_mode = os.lstat(path)[stat.ST_MODE]
			
			if prev_name_data.has_key(name):
				cur_prev_nums = prev_name_data[name]
			else:
				cur_prev_nums = []
			
			try:
				if stat.S_ISLNK(file_mode):
					node = Symlink(self.backup,self,name)
					node.set_level(self.get_level())
					node.set_number(ctx.next_num())
					node.scan(ctx,cur_prev_nums)
					self.cur_data[name] = (node.get_level(),node.get_type(),node.get_number())
					if node.get_level() >= self.get_level():
						# If node is not based, then it is new
						self.modified = True
					self.dirty = True
				elif stat.S_ISREG(file_mode):
					node = File(self.backup,self,name)
					node.set_level(self.get_level())
					node.set_number(ctx.next_num())
					same = node.scan(ctx,cur_prev_nums)
					self.cur_data[name] = (node.get_level(),node.get_type(),node.get_number())
					if node.get_level() >= self.get_level():
						self.modified = True
					self.dirty = True
				elif stat.S_ISDIR(file_mode):
					node = Directory(self.backup,self,name)
					node.set_level(self.get_level())
					node.set_number(ctx.next_num())
					# The order of append and scan is different here,
					# to make sure that the intermediate dir info is saved.
					self.subdirs.append(node)
					# The written level and number are temporary, the directory might
					# decide that it is based during its scan, in which case its level
					# and number will change
					self.cur_data[name] = (node.get_level(),node.get_type(),node.get_number())
					same = node.scan(ctx,cur_prev_nums)
					self.cur_data[name] = (node.get_level(),node.get_type(),node.get_number())
					if node.get_level() >= self.get_level():
						self.modified = True
					self.dirty = True
				else:
					print "Ignoring unrecognized file type", path
			except OSError:
				print "OSError accessing", path
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno,strerror), path
				traceback.print_exc()

		#
		# Consider all the children scanned so far
		#
		self.base_level = None
		if self.modified:
			# OK, modification already noticed. No need for additional checks
			self.write(ctx)
		else:
			# scan the previous versions of this dir, to find which of them have
			# the same contents as this one. Base on the latest one that is basable
			equal_levels = [level for level,data in prev_level_data.iteritems()
				if ctx.is_base_level(level) and data==cur_data]
			assert len(equal_levels) <= 1
			if len(equal_levels) == 1:
				# Found a prev dir to base on!
				self.level,self.number = prev_level_file_info(equal_levels[0])
			else:
				self.write(ctx)

		# Remove the subdirs list to avoid holding them all in memory
		self.subdirs = None
	def flush(self,ctx):
		"""
		Flush the contents of the current node.
		Called when a container is completed.
		"""
		if self.modified:
			# Nothing has happened since last flush
			for subdir in self.subdirs:
				subdir.flush(ctx)
			self.write(ctx)
			self.dirty = False

	def write(self,ctx):
		"""
		Write the info of the current dir to database
		"""
		valueS = StringIO()
		# sorting is an optimization to make everybody access files in the same order,
		# TODO: measure if this really makes things faster (probably will with a btree db)
		for name in sorted(self.cur_data.items()):
			(level,the_type,number) = self.cur_data[name]
			Format.write_int(valueS,node.encode(the_type,level))
			Format.write_int(valueS,number)
			Format.write_string(valueS,name)
			
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		ctx.new_stats_db[key] = Format.serialize_ints(self.stat())
		
	def restore(self,ctx):
		if self.parent != None:
			print "Restoring dir", self.path(), self.number
			os.mkdir(self.path())

		files_db = self.get_files_db(ctx,self.get_level())
		stats_db = self.get_stats_db(ctx,self.get_level())
		key = self.get_key()
		valueS = StringIO(files_db[key])
		for (code,number,name) in read_directory_entries(valueS):
			the_type = node_type(code)
			level = node_level(code)
			if the_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self,name)
			elif the_type == NODE_TYPE_FILE:
				node = File(self.backup,self,name)
			elif the_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup,self,name)
			else:
				raise "Unknown node type [%s]"%the_type

			node.set_number(number)
			node.set_level(level)
			node.restore(ctx)
		stats = stats_db[key]
		self.restore_stats(stats)

	def request_blocks(self,ctx):
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
			node.set_number(node_num)
			node.code = node_type
			node.request_blocks(ctx,based)

	def list_files(self,ctx):
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
			node.set_number(node_num)
			node.code = node_type
			#print "Child node", node_name, "code:", node_type
			node.list_files(ctx,based)
