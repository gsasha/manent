import os, os.path, stat
import base64
from cStringIO import StringIO
import re

import Backup
from Config import Config

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

	def path(self):
		pathElements = []
		node = self
		while node != None:
			pathElements.append(node.name)
			node = node.parent
		pathElements.reverse()
		return "/".join(pathElements)

	#
	# Node serialization to db
	#
	def flush(self,ctx):
		pass

	def set_num(self,num):
		self.number = num
	def get_key(self):
		return self.backup.config.node_key(self.number)

#--------------------------------------------------------
# CLASS:File
#--------------------------------------------------------
class File(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	#
	# Scanning and restoring
	#
	def scan(self,ctx,prev_nums):
		print "scanning", self.path()
		#
		# Check if we have seen this file already
		#
		file_stat = os.stat(self.path())
		file_stat_str = " ".join(str(x) for x in file_stat)
		inode_num = file_stat[stat.ST_INO]
		nlink = file_stat[stat.ST_NLINK]
		if nlink > 1:
			if ctx.inodes_db.has_key(inode_num):
				# This is a hard link to already existing file.
				# Ignore the suggested file number
				self.number = ctx.inodes_db[inode_num]
				print "  is a hard link to file", self.number
				return
			# Although file is apparently a hard link, we've not seen it yet
			ctx.inodes_db[inode_num] = self.number
		
		#
		# See if file was in previous increments
		#
		for (db_num,file_num) in prev_nums:
			# TODO: this must work only if the file is old enough
			print "  found in increment %d" %(db_num),
			
			old_db = ctx.prev_files_dbs[db_num]
			old_key = self.backup.config.node_key(file_num)
			old_stat_str = old_db["S"+old_key]
			old_stat = [int(s) for s in re.split("\s+",old_stat_str)]
			if (file_stat[stat.ST_INO]==old_stat[stat.ST_INO]) and \
			            (file_stat[stat.ST_MTIME]==old_stat[stat.ST_MTIME]):
				key = self.get_key()
				ctx.new_files_db[key] = old_db[old_key]
				ctx.new_files_db["S"+key] = old_db["S"+old_key]
				print "mtime", file_stat[stat.ST_MTIME], "reusing"
				return
			else:
				print "but stamp differs:", file_stat_str, "!=", old_stat_str
			
		#
		# File not yet in database, process it
		#
		digests = []
		offset = 0
		read_handle = open(self.path(), "rb")
		while True:
			data = read_handle.read(self.backup.config.blockSize())
			if len(data)==0:
				break
			digest = self.backup.config.dataDigest(data)
			digests.append(digest)
			ctx.add_block(data,digest)
			#print "Storing block %d of %s, [%d:%d] digest:%s" % (index, self.path(), offset,len(data), base64.b64encode(digest))
			offset += len(data)
		#
		# Serialize to the filesystem db
		#
		valueS = StringIO()
		if nlink>1:
			valueS.write("H")
		else:
			valueS.write("F")
		for digest in digests:
			valueS.write(digest)
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		ctx.new_files_db["S"+key] = file_stat_str
		
	def restore(self,ctx):
		key = self.get_key()
		valueS = StringIO(ctx.files_db[key])
		linktype = valueS.read(1)
		#
		# Check if this file is a hard link to already
		# existing one
		#
		if linktype=="H":
			if ctx.inodes_db.has_key(self.number):
				otherFile = ctx.inodes_db[num]
				print "Restoring hard link from", otherFile, "to", self.path()
				os.link(otherFile, self.path())
				return
			ctx.inodes_db[num] = self.path()

		#
		# No, this file is new. Create it.
		#
		digests = []
		digestSize = self.backup.config.dataDigestSize()
		while True:
			digest = valueS.read(digestSize)
			if digest=='':
				break
			digests.append(digest)
		
		print "Restoring file", self.path()
		file = open(self.path(), "w")
		for digest in digests:
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(self.backup.read_block(digest))
	
	def request_blocks(self,ctx,block_cache):
		key = self.get_key()
		valueS = StringIO(ctx.files_db[key])
		linktype = valueS.read(1)

		if linktype=="H":
			if ctx.inodes_db.has_key(num):
				# This file is a hard link, so it needs no blocks
				return
			ctx.inodes_db[num] = self.path()
		#
		# Ok, this file is new. Count all its blocks
		#
		digestSize = self.backup.config.dataDigestSize()
		while True:
			digest = valueS.read(digestSize)
			if digest == "": break
			block_cache.request_block(digest)
	def list_files(self,db):
		print self.path()
#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def scan(self,ctx,prev_nums):
		self.link = os.readlink(self.path())
		print "scanning", self.path(), "->", self.link
		key = self.get_key()
		ctx.new_files_db[key] = self.link
	def restore(self,ctx):
		key = self.get_key()
		self.link = ctx.files_db[key]
		print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
	def list_files(self,db):
		print self.path()

##--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def scan(self,ctx,prev_nums):
		#
		# Reload data from previous increments
		#
		print "Path", self.path(), "found in previous increments:", prev_nums
		prev_data = {}
		for (db_num,file_num) in prev_nums:
			db = ctx.prev_files_dbs[db_num]
			file_key = self.backup.config.node_key(file_num)
			if not db.has_key(file_key):
				print "DB %d doesn't have key %s" % (db_num,file_key)
				continue
			valueS = StringIO(db[file_key])
			while True:
				node_type = valueS.read(1)
				if node_type == "":
					break
				node_num = self.backup.config.read_int(valueS)
				node_name = self.backup.config.read_string(valueS)
				if not prev_data.has_key(node_name):
					prev_data[node_name] = []
				prev_data[node_name].append((db_num,node_num))
		#print "Prev data:"
		#keys = prev_data.keys()
		#keys.sort()
		#for key in keys:
			#print " ", key, prev_data[key]
		#
		# Scan the directory
		#
		# TODO: there shouldn't be two lists
		print "starting scan for", self.path()
		self.modified = True
		self.children = []
		for name in os.listdir(self.path()):
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
				elif stat.S_ISREG(file_mode):
					node = File(self.backup,self,name)
					node.set_num(ctx.next_num())
					node.scan(ctx,cur_prev_nums)
					self.children.append(node)
				elif stat.S_ISDIR(file_mode):
					node = Directory(self.backup,self,name)
					node.set_num(ctx.next_num())
					# The order of append and scan is different here!
					self.children.append(node)
					node.scan(ctx,cur_prev_nums)
			except OSError:
				print "OSError accessing", path
			except IOError:
				print "IOError accessing", path
				
		self.flush(ctx)
		# remove the children list - we don't want to keep everything in memory
		# while scanning
		self.child_nodes = None
	def flush(self,ctx):
		"""
		Flush the contents of the current node.
		Called when a container is completed or when
		the node is completed
		"""
		for child in self.children:
			child.flush(ctx)
		
		if not self.modified:
			return

		valueS = StringIO()
		for child in self.children:
			if isinstance(child,Directory):
				valueS.write("D")
			elif isinstance(child,File):
				valueS.write("F")
			elif isinstance(child,Symlink):
				valueS.write("S")
			else:
				raise "Unrecognized object flushing"
			
			self.backup.config.write_int(valueS,child.number)
			self.backup.config.write_string(valueS,child.name)
			
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		self.modified = False
		print "Flushing node", self.path(), "to", key
		
	def restore(self,ctx):
		if self.path() != ".":
			print "Restoring dir", self.path(), self.number
			os.mkdir(self.path())
		key = self.get_key()
		valueS = StringIO(ctx.files_db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				# no more entries in this dir
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self.backup,self, node_name)
			elif node_type == "F":
				node = File(self.backup,self,node_name)
			elif node_type == "S":
				node = Symlink(self.backup,self,node_name)
			else:
				raise "Unknown node type [%s]"%node_type

			node.set_num(node_num)
			node.restore(ctx)

	def request_blocks(self,ctx,block_cache):
		#print "Requesting blocks in", self.path()
		key = self.get_key()
		#print "loading block", self.number, "key", key
		valueS = StringIO(ctx.files_db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self.backup,self,node_name)
			elif node_type == "F":
				node = File(self.backup,self,node_name)
			elif node_type == "S":
				# Nothing to do for symlinks
				continue
			else:
				raise "Unknown node type [%s]"%node_type
			node.set_num(node_num)
			node.request_blocks(ctx,block_cache)

	def list_files(self,db):
		print self.path()
		key = self.get_key()
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self.backup,self,node_name)
			elif node_type == "F":
				node = File(self.backup,self,node_name)
			elif node_type == "S":
				node = Symlink(self.backup,self,node_name)
			node.set_num(node_num)
			node.list_files(db)
