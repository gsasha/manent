import base64
import os, os.path
import re
import stat
import cStringIO as StringIO
import time
import traceback

import Backup
import Container
import utils.Digest as Digest
import utils.Format as Format
import utils.FileIO as FileIO
import utils.IntegerEncodings as IntegerEncodings
import PackerStream

#----------------------------------------------------
# Node conversion
#----------------------------------------------------
NODE_TYPE_DIR     = 0
NODE_TYPE_FILE    = 1
NODE_TYPE_SYMLINK = 2

STAT_PRESERVED_MODES = [
	stat.ST_MODE,
	stat.ST_UID,
	stat.ST_GID,
	stat.ST_MTIME,
	stat.ST_CTIME,
	stat.ST_ATIME,
	stat.ST_NLINK,
	stat.ST_INO]

NULL_STAT = {}
for s in STAT_PRESERVED_MODES:
	NULL_STAT[s] = 0
# NULL_STAT = {0:s for s in []}
# NULL_STAT = {a:b for a,b in {}.iteritems()}

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
		self.stats = None
		self.cached_path = None
	def get_digest(self):
		return self.digest
	def set_digest(self,digest):
		self.digest = digest
	#
	# Path computations
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
	def get_name(self):
		return self.name
	#
	# Stat handling
	#
	def compute_stats(self):
		"""
		Compute the os.stat data for the current file
		"""
		node_stat = os.lstat(self.path())
		self.stats = {}
		for mode in STAT_PRESERVED_MODES:
			self.stats[mode] = node_stat[mode]
	def set_stats(self,stats):
		self.stats = stats
	def get_stats(self):
		return self.stats
	
	def serialize_stats(self,base_stats):
		stats = self.get_stats()
		file = StringIO.StringIO()
		if base_stats is not None:
			for mode in STAT_PRESERVED_MODES:
				Format.write_int(file,stats[mode]-base_stats[mode])
		else:
			for mode in STAT_PRESERVED_MODES:
				Format.write_int(file,stats[mode])
		return file.getvalue()
		
	def unserialize_stats(self,file,base_stats):
		stats = {}
		if base_stats is not None:
			for mode in STAT_PRESERVED_MODES:
				val = Format.read_int(file)
				stats[mode] = base_stats[mode]+val
		else:
			for mode in STAT_PRESERVED_MODES:
				val = Format.read_int(file)
				stats[mode] = val
		return stats
	
	#-----------------------------------------------------
	# Support for scanning hard links:
	# 
	# see if the current file is a hard link to another file
	# that has already been scanned. If so, reuse it.
	#
	def scan_hlink(self,ctx):
		if self.stats[stat.ST_NLINK] == 1:
			return False
		inode_num = self.stats[stat.ST_INO]
		if ctx.inodes_db.has_key(inode_num):
			self.digest = ctx.inodes_db[inode_num]
			return True
		return False
	def update_hlink(self,ctx):
		if self.stats[stat.ST_NLINK] == 1:
			return
		inode_num = self.stats[stat.ST_INO]
		if ctx.inodes_db.has_key(inode_num):
			return
		ctx.inodes_db[inode_num] = self.digest
	def restore_hlink(self,ctx,dryrun=False):
		if self.stats[stat.ST_NLINK] == 1:
			return False
		if not ctx.inodes_db.has_key(self.digest):
			ctx.inodes_db[self.digest] = self.path()
			return False
		if not dryrun:
			otherFile = ctx.inodes_db[self.digest]
			os.link(otherFile, self.path())
		return True
		
	#
	# Support for scanning in previous increments
	#
	def scan_prev(self, ctx, prev_nums):
		"""
		"""
		ctx.total_nodes += 1
		
		for (prev_type,prev_stat,prev_digest) in reversed(prev_nums):
			if prev_type != self.get_type():
				print "  node type differs in the db"
				break
			#if stat.S_IFMT(self.stats[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
				#print "  Node type differs in the fs"
				#break
			if prev_stat is None:
				print "  Base stat not defined"
				break
			if self.stats[stat.ST_INO] != prev_stat[stat.ST_INO]:
				print "  Inode number differs: was %d, now %d" % (file_stat[stat.ST_INO],old_stat[stat.ST_INO]), file_stat
				break
			if self.stats[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
				print "  Mtime differs: %d != %d" % (self.stats[stat.ST_MTIME], prev_stat[stat.ST_MTIME])
				break
			if time.time() - self.stats[stat.ST_MTIME] <= 1.0:
				# The time from the last change is less than the resolution
				# of time() functions
				print "  File too recent",prev_stat[stat.ST_MTIME],time.time()
				break
			
			#
			# OK, the prev node seems to be the same as this one.
			# Reuse it.
			#
			self.stats = prev_stat
			self.digest = prev_digest
			return True

		#print "changed node", self.path()
		ctx.changed_nodes += 1
		return False
	def restore_stats(self,
		              restore_chmod=True,
					  restore_chown=True,
		              restore_utime=True):
		if restore_chmod:
			os.chmod(self.path(),self.stats[stat.ST_MODE])
		if restore_chown:
			os.lchown(self.path(),self.stats[stat.ST_UID],
				      self.stats[stat.ST_GID])
		if restore_utime:
			os.utime(self.path(),(self.stats[stat.ST_ATIME],
				                  self.stats[stat.ST_MTIME]))

#--------------------------------------------------------
# CLASS:File
#--------------------------------------------------------
class File(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def get_type(self):
		return NODE_TYPE_FILE
	#
	# Scanning and restoring
	#
	def scan(self, ctx, prev_nums):
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
		packer = PackerStream.PackerOStream(self.backup, Container.CODE_DATA)
		for data in FileIO.read_blocks(open(self.path(), "rb"),
			                    self.backup.get_block_size()):
			packer.write(data)
			
		self.digest = packer.get_digest()
		self.update_hlink(ctx)

	def restore(self, ctx):
		"""
		Recreate the data from the information stored in the
		backup
		"""
		
		#
		# Check if the file has already been processed
		# during this pass
		#
		if self.restore_hlink(ctx):
			return

		#
		# No, this file is new. Create it.
		#
		packer = PackerStream.PackerIStream(self.backup, self.digest)
		file = open(self.path(), "wb")
		for data in FileIO.read_blocks(packer, Digest.dataDigestSize()):
			#print "File", self.path(), "reading digest",
			#    base64.b64encode(digest)
			file.write(data)
		file.close()
		
		self.restore_stats()
	
	def request_blocks(self, ctx):
		"""
		Put requests for the blocks of the file into the blocks cache
		"""
		key = self.get_key()
		stats = Format.deserialize_ints(stats_db[key])

		#
		# Check if the file has already been processed
		# during this pass
		#
		if self.restore_hlink(ctx, stats, dryrun=True):
			return
		
		#
		# Ok, this file is new. Count all its blocks
		#
		valueS = StringIO.StringIO(db[key])
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			ctx.request_block(digest)
	def list_files(self,ctx):
		print "F", self.path(), self.get_digest()

#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self, backup, parent, name):
		Node.__init__(self, backup, parent, name)
	def get_type(self):
		return NODE_TYPE_SYMLINK
	def scan(self, ctx, prev_nums):
		if self.scan_hlink(ctx):
			return

		if self.scan_prev(ctx,prev_nums):
			return

		self.link = os.readlink(self.path())

		packer = PackerStream.PackerOStream(self.backup, Container.CODE_DATA)
		packer.write(self.link)

		self.digest = packer.get_digest()
		self.update_hlink(ctx)
		
	def restore(self,ctx):
		if self.restore_hlink(ctx):
			return

		packer = PackerStream.PackerIStream(self.backup, self.digest)
		self.link = packer.read()
		os.symlink(self.link, self.path())
		# on Linux, there is no use of the mode of a symlink
		# and no way to restore the times of the link itself
		self.restore_stats(restore_chmod=False, restore_utime=False)

	def list_files(self, ctx):
		print "S", self.path(), self.get_digest()

#--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
	def __init__(self, backup, parent, name):
		Node.__init__(self, backup, parent, name)
	def get_type(self):
		return NODE_TYPE_DIR
	def scan(self, ctx, prev_nums, exclusion_processor):
		"""Scan the node, considering data in all the previous increments
		"""
		#
		# Process data from previous increments.
		#
		ctx.total_nodes += 1
		# prev data indexed by file, for directory scan
		prev_name_data = {}

		for (prev_type, prev_stat, prev_digest) in prev_nums:
			if prev_type is not None and prev_type != self.get_type():
				# This previous entry is not a directory.
				# Definitely shouldn't read it.
				break

			dir_stream = PackerStream.PackerIStream(self.backup, prev_digest)
			for node_type, node_name, node_stat, node_digest in\
			      self.read_directory_entries(dir_stream,prev_stat):
				if not prev_name_data.has_key(node_name):
					prev_name_data[node_name] = []
				prev_name_data[node_name].append(
					(node_type, node_stat, node_digest))

		last_type, last_stat, last_digest = None, None, None
		if len(prev_nums) != 0:
			last_type, last_stat, last_digest = prev_nums[-1]

		#
		# Initialize scanning data
		#
		self.children = []
		
		#
		# Scan the directory
		#
		#print "starting scan for", self.path()
		exclusion_processor.filter_files()
		for name in exclusion_processor.get_included_files():
			path = os.path.join(self.path(),name)
			file_mode = os.lstat(path)[stat.ST_MODE]

			if prev_name_data.has_key(name):
				cur_prev = prev_name_data[name]
			else:
				cur_prev = []

			try:
				if stat.S_ISLNK(file_mode):
					node = Symlink(self.backup, self, name)
					node.compute_stats()
					node.scan(ctx, cur_prev)
					self.children.append(node)
				elif stat.S_ISREG(file_mode):
					node = File(self.backup, self, name)
					node.compute_stats()
					node.scan(ctx,cur_prev)
					self.children.append(node)
				else:
					print "Ignoring unrecognized file type", path
			except OSError:
				print "OSError accessing", path
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno, strerror), path
				traceback.print_exc()

		for name in exclusion_processor.get_included_dirs():
			path = os.path.join(self.path(), name)
			file_mode = os.lstat(path)[stat.ST_MODE]

			if prev_name_data.has_key(name):
				cur_prev = prev_name_data[name]
			else:
				cur_prev = []

			try:
				if stat.S_ISDIR(file_mode):
					node = Directory(self.backup, self, name)
					#
					# The order is different here, and it's all because directory can
					# produce temporary digest of its contents during scanning
					#
					node.compute_stats()
					self.children.append(node)
					child_ep = exclusion_processor.descend(name)
					node.scan(ctx, cur_prev, child_ep)
				else:
					print "Ignoring unrecognized file type", path
			except OSError:
				print "OSError accessing", path
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno, strerror), path
				traceback.print_exc()

		self.write(ctx)
		self.children = None

		if self.digest != last_digest:
			#print "changed node", self.path()
			ctx.changed_nodes += 1
	def flush(self, ctx):
		"""
		Flush the contents of the current node.
		Called when a container is completed.
		"""
		dirty = False
		for child in self.children:
			if child.get_type() == NODE_TYPE_DIR:
				prev_digest = child.get_digest()
				child.flush()
				curr_digest = child.get_digest()
				if prev_digest != curr_digest:
					dirty = True
		if dirty:
			self.write(ctx)

	def write(self, ctx):
		"""
		Write the info of the current dir to database
		"""
		packer = PackerStream.PackerOStream(self.backup, Container.CODE_DIR)
		# sorting is an optimization to make everybody access files in the same order,
		# TODO: measure if this really makes things faster (probably will with a btree db)
		for child in self.children:
			Format.write_int(packer,child.get_type())
			Format.write_string(packer,child.get_name())
			packer.write(child.get_digest())
			stats_str = child.serialize_stats(self.get_stats())
			packer.write(stats_str)
		
		self.digest = packer.get_digest()
		
	def restore(self, ctx):
		if self.parent != None:
			os.mkdir(self.path())

		packer = PackerStream.PackerIStream(self.backup, self.get_digest())
		for (node_type, node_name, node_stat, node_digest) in self.read_directory_entries(packer):
			if node_type == NODE_TYPE_DIR:
				node = Directory(self.backup, self, node_name)
			elif node_type == NODE_TYPE_FILE:
				node = File(self.backup, self, node_name)
			elif node_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup, self, node_name)
			else:
				raise Exception("Unknown node type [%s]"%node_type)
			node.set_stats(node_stat)
			node.set_digest(node_digest)
			node.restore(ctx)
		if self.stats is not None:
			# Root node has no stats
			self.restore_stats()

	def list_files(self,ctx):
			
		print self.path()
		packer = PackerStream.PackerIStream(self.backup,self.get_digest())
		for (node_type,node_name,node_stat,node_digest) in self.read_directory_entries(packer):
			if node_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self,node_name)
			elif node_type == NODE_TYPE_FILE:
				node = File(self.backup,self,node_name)
			elif node_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup,self,node_name)
			node.set_stats(node_stat)
			node.set_digest(node_digest)
			node.list_files(ctx)
	
	def read_directory_entries(self,file,base_stats=None):
		if base_stats is None:
			base_stats = self.stats
		while True:
			node_type = Format.read_int(file)
			if node_type is None:
				raise StopIteration
			node_name = Format.read_string(file)
			node_digest = file.read(Digest.dataDigestSize())
			node_stat = self.unserialize_stats(file,base_stats)
			yield (node_type,node_name,node_stat,node_digest)
