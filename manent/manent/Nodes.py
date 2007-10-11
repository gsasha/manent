import os, os.path, stat, time
import base64
from cStringIO import StringIO
import re
import manent.utils.IntegerEncodings as IntegerEncodings
import manent.utils.Digest as Digest
import manent.utils.Format as Format
from manent.utils.FileIO import read_blocks
from PackerStream import *
import traceback

import Backup

#----------------------------------------------------
# Node conversion
#----------------------------------------------------
NODE_TYPE_DIR     = 0
NODE_TYPE_FILE    = 1
NODE_TYPE_SYMLINK = 2

STAT_PRESERVED_MODES = [stat.ST_MODE, stat.ST_UID, stat.ST_GID, stat.ST_MTIME, stat.ST_CTIME, stat.ST_ATIME, stat.ST_NLINK, stat.ST_INO]

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
		self.stats = {}
		node_stat = os.lstat(self.path())
		for mode in STAT_PRESERVED_MODES:
			self.stats[mode] = node_stat[mode]
	def set_stats(self,stats):
		self.stats = stats
	def get_stats(self):
		return self.stats
	
	def serialize_stats(self,base_stats):
		stats = self.get_stats()
		file = StringIO()
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
	def scan_prev(self,ctx,prev_nums):
		"""
		"""
		ctx.total_nodes += 1
		
		for (prev_stat,prev_digest) in reversed(prev_nums):
			if stat.S_IFMT(self.stats[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
				#print "  Node type differs"
				break
			if self.stats[stat.ST_INO] != prev_stat[stat.ST_INO]:
				#print "  Inode number differs: was %d, now %d" % (file_stat[stat.ST_INO],old_stat[stat.ST_INO]), file_stat
				break
			if self.stats[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
				#print "  Mtime differs"
				break
			if time.time() - self.stats[stat.ST_MTIME] <= 1.0:
				# The time from the last change is less than the resolution
				# of time() functions
				#print "  File too recent",file_stat[stat.ST_MTIME],time.time()
				break
			
			#
			# OK, the prev node seems to be the same as this one.
			# Reuse it.
			#
			self.stats = prev_stat
			self.digest = prev_digest
			return True
		
		return False
	def restore_stats(self):
		os.chmod(self.path(),self.stats[stat.ST_MODE])
		os.chown(self.path(),self.stats[stat.ST_UID],self.stats[stat.ST_GID])
		os.utime(self.path(),(self.stats[stat.ST_ATIME],self.stats[stat.ST_MTIME]))

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
		packer = PackerOStream(self.backup,Container.CODE_DATA)
		for data in read_blocks(open(self.path(), "rb"), self.backup.container_config.blockSize()):
			packer.write(data)
			
		self.digest = packer.get_digest()
		self.update_hlink(ctx)

	def restore(self,ctx):
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
		packer = PackerIStream(self.backup,self.digest)
		file = open(self.path(), "wb")
		for data in read_blocks(packer,Digest.dataDigestSize()):
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(data)
		file.close()
		
		self.restore_stats()
	
	def request_blocks(self,ctx):
		"""
		Put requests for the blocks of the file into the blocks cache
		"""
		key = self.get_key()
		stats = Format.deserialize_ints(stats_db[key])

		#
		# Check if the file has already been processed
		# during this pass
		#
		if self.restore_hlink(ctx,stats,dryrun=True):
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
		packer = PackerOStream(self.backup, Container.CODE_DATA)
		packer.write(self.link)

		self.digest = packer.get_digest()
		self.update_hlink(ctx)
		
	def restore(self,ctx):
		if self.restore_hlink(ctx):
			return

		packer = PackerIStream(self.backup, self.digest)
		self.link = packer.read()
		#print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
		self.restore_stats()

	def list_files(self,ctx):
		(node_code,node_level) = node_decode(self.code)
		if node_level is not None:
			print "B%d" % node_level,
		elif base_level is not None:
			print "B%d" % base_level,
		print self.path(), self.number, self.link

#--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def get_type(self):
		return NODE_TYPE_DIR
	def scan(self,ctx,prev_nums):
		"""Scan the node, considering data in all the previous increments
		"""
		#
		# Reload data from previous increments.
		#
		
		#print "Path", self.path(), "found in previous increments:", prev_nums
		# prev data indexed by file, for directory scan
		prev_name_data = {}
		
		for (prev_type,prev_stat,prev_digest) in prev_nums:
			if prev_type is not None and prev_type != self.get_type():
				# This previous entry is not a directory.
				# Definitely shouldn't read it.
				break

			dir_stream = PackerIStream(self.backup, prev_digest)
			for type_str,name,stat_str,digest in self.read_directory_entries(dir_stream):
				if not prev_name_data.has_key(name):
					prev_name_data[name] = []
				prev_name_data[name].append((int(type_str),stat_str,digest))

		#
		# Initialize scanning data
		#
		self.children = []
		
		#
		# Scan the directory
		#
		#print "starting scan for", self.path()
		entries = os.listdir(self.path())
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
					node.compute_stats()
					node.scan(ctx,cur_prev_nums)
					self.children.append(node)
				elif stat.S_ISREG(file_mode):
					node = File(self.backup,self,name)
					node.compute_stats()
					node.scan(ctx,cur_prev_nums)
					self.children.append(node)
				elif stat.S_ISDIR(file_mode):
					node = Directory(self.backup,self,name)
					#
					# The order is different here, and it's all because directory can
					# produce temporary digest of its contents during scanning
					#
					self.children.append(node)
					node.compute_stats()
					node.scan(ctx,cur_prev_nums)
				else:
					print "Ignoring unrecognized file type", path
			except OSError:
				print "OSError accessing", path
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno,strerror), path
				traceback.print_exc()

		self.write(ctx)
		self.children = None
	def flush(self,ctx):
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

	def read_directory_entries(self,file):
		while True:
			node_type = Format.read_int(file)
			if node_type is None:
				raise StopIteration
			node_name = Format.read_string(file)
			node_digest = file.read(Digest.dataDigestSize())
			node_stat = self.unserialize_stats(file,self.stats)
			yield (node_type,node_name,node_digest,node_stat)
	def write(self,ctx):
		"""
		Write the info of the current dir to database
		"""
		packer = PackerOStream(self.backup,Container.CODE_DIR)
		# sorting is an optimization to make everybody access files in the same order,
		# TODO: measure if this really makes things faster (probably will with a btree db)
		for child in self.children:
			Format.write_int(packer,child.get_type())
			Format.write_string(packer,child.get_name())
			packer.write(child.get_digest())
			stats_str = child.serialize_stats(self.get_stats())
			packer.write(stats_str)
		
		self.digest = packer.get_digest()
		
	def restore(self,ctx):
		if self.parent != None:
			os.mkdir(self.path())

		packer = PackerIStream(self.backup,self.get_digest())
		for (node_type,node_name,node_digest,node_stat) in self.read_directory_entries(packer):
			if node_type == NODE_TYPE_DIR:
				node = Directory(self.backup,self,node_name)
			elif node_type == NODE_TYPE_FILE:
				node = File(self.backup,self,node_name)
			elif node_type == NODE_TYPE_SYMLINK:
				node = Symlink(self.backup,self,node_name)
			else:
				raise Exception("Unknown node type [%s]"%node_type)
			node.set_stats(node_stat)
			node.set_digest(node_digest)
			node.restore(ctx)
		if self.stats is not None:
			# Root node has no stats
			self.restore_stats()

	def list_files(self,ctx):
			
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
