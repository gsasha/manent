import os, os.path, stat
import base64
from cStringIO import StringIO
import re

import Backup
from Config import Config

NODE_DIR           = "D"
NODE_FILE          = "F"
NODE_SYMLINK       = "S"
NODE_DIR_BASED     = "E"
NODE_FILE_BASED    = "G"
NODE_SYMLINK_BASED = "T"

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
		def strip_L(s):
			if s[-1]=='L':
				return s[:-1]
			return s
		file_stat_str = " ".join(strip_L("%x"%x) for x in file_stat)
		inode_num = file_stat[stat.ST_INO]
		nlink = file_stat[stat.ST_NLINK]
		if nlink > 1:
			if ctx.inodes_db.has_key(inode_num):
				# This is a hard link to already existing file.
				# Ignore the suggested file number
				(self.number,self.code) = ctx.inodes_db[inode_num]
				print "  is a hard link to file", self.number
				return
			# Although file is apparently a hard link, we've not seen it yet
			ctx.inodes_db[inode_num] = self.number
		
		#
		# See if file was in previous increments
		#
		for (db_num,file_num,file_code) in prev_nums:
			# TODO: this must work only if the file is old enough
			print "  found file %d in increment %d" %(file_num,db_num),
			# Decide where to look for the node
			if file_code == NODE_FILE:
				old_db = ctx.prev_files_dbs[db_num]
			elif file_code == NODE_FILE_BASED:
				old_db = ctx.base_files_db
			else:
				raise "File cannot have data of code", file_code

			# load the old data
			old_key = self.backup.config.node_key(file_num)
			if not old_db.has_key("S"+old_key):
				# key is not there, probably because it is in the based file?
				#print "Failed  to load", old_key, "from", db_num, ":", file_num, ":", file_code
				continue
			else:
				#print "Success to load", old_key, "from", db_num, ":", file_num, ":", file_code
				pass
				
			old_stat_str = old_db["S"+old_key]
			old_stat = [int(s,16) for s in re.split("\s+",old_stat_str)]
			if (file_stat[stat.ST_INO]==old_stat[stat.ST_INO]) and \
			            (file_stat[stat.ST_MTIME]==old_stat[stat.ST_MTIME]):
				key = self.get_key()
				print "mtime", file_stat[stat.ST_MTIME], "reusing"

				prev_value = old_db[old_key]
				ctx.total_nodes += 1
				if file_code == NODE_FILE_BASED:
					#print "File contants are the same as in base!"
					# Don't write data to current DB, instead, the parent dir
					# will write a reference to the base file number
					self.number = file_num
					self.code = NODE_FILE_BASED
				else:
					#print "File contents, found in prev increment, are not in the base!"
					# File contents in the previous increment are different from
					# those of the base (and thus the file is not of FILE_BASED type).
					# In this case, we still add the file to current DB, and count it
					# as changed, to make sure next time we'll probably start an unbased
					# increment
					ctx.changed_nodes += 1
					ctx.new_files_db[key] = old_db[old_key]
					ctx.new_files_db["S"+key] = old_db["S"+old_key]
					self.code = NODE_FILE

				# Before exiting, must check if we have to update inodes_db
				if nlink > 1:
					# Although file is apparently a hard link, we've not seen it yet
					ctx.inodes_db[inode_num] = (self.number,self.code)
				
				return
			else:
				print "but stamp differs:", file_stat_str, "!=", old_stat_str
		
		#
		# File not yet in database, process it
		#
		#print "File contents differ from any base!"
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
		value = valueS.getvalue()
		key = self.get_key()
		ctx.new_files_db[key] = value
		ctx.new_files_db["S"+key] = file_stat_str

		ctx.total_nodes += 1
		ctx.changed_nodes += 1
		self.code = NODE_FILE
		return
		
	def restore(self,ctx,based):
		if self.code == NODE_FILE_BASED or based==True:
			db = ctx.base_files_db
		else:
			db = ctx.files_db
			
		key = self.get_key()
		valueS = StringIO(db[key])
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
		file = open(self.path(), "wb")
		for digest in digests:
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(self.backup.read_block(digest))
	
	def request_blocks(self,ctx,block_cache,based):
		if self.code == NODE_FILE_BASED or based==True:
			db = ctx.base_files_db
		else:
			db = ctx.files_db
		key = self.get_key()
		valueS = StringIO(db[key])
		
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
	def list_files(self,ctx,based):
		if self.code == NODE_FILE_BASED or based == True:
			print "B",
		print self.path()

#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self,backup,parent,name):
		Node.__init__(self,backup,parent,name)
	def scan(self,ctx,prev_nums):
		self.link = os.readlink(self.path())
		ctx.total_nodes += 1
		print "scanning", self.path(), "->", self.link
		key = self.get_key()
		for (db_num,file_num,file_code) in prev_nums:
			if file_code == NODE_SYMLINK:
				# If the symlink in a prev increment is not based,
				# we don't reuse it.
				continue
			elif file_code == NODE_SYMLINK_BASED:
				# OK, if this is equal, we'll be able to base on it
				old_db = ctx.base_files_db
			else:
				raise "File cannot have data of code", file_code

			if old_db[self.backup.config.node_key(file_num)] == self.link:
				self.number = file_num
				self.code = NODE_SYMLINK_BASED
				return

		self.code = NODE_SYMLINK
		ctx.changed_nodes += 1
		ctx.new_files_db[key] = self.link
		
	def restore(self,ctx,based):
		print "Restoring symlink in", self.path()
		if self.code == NODE_SYMLINK_BASED or based==True:
			db = ctx.base_files_db
		else:
			db = ctx.files_db
		key = self.get_key()
		self.link = db[key]
		print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
	def list_files(self,ctx,based):
		if self.code == NODE_SYMLINK_BASED or based == True:
			print "B",
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
		base_nodes = {}
		base_num = None
		for (db_num,file_num,file_code) in prev_nums:
			if file_code == NODE_DIR_BASED:
				base_num = file_num
			
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

				# If the file sits in a database which is a "base" one,
				# upgrade the status of the prev file to note that
				if file_code == NODE_DIR_BASED:
					if node_type==NODE_FILE:
						node_type = NODE_FILE_BASED
						base_nodes[node_name] = 1
					elif node_type==NODE_SYMLINK:
						node_type = NODE_SYMLINK_BASED
						base_nodes[node_name] = 1
					elif node_type==NODE_DIR:
						node_type = NODE_DIR_BASED
						base_nodes[node_name] = 1

				if not prev_data.has_key(node_name):
					prev_data[node_name] = []
				prev_data[node_name].append((db_num,node_num,node_type))
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
					if node.code == NODE_SYMLINK:
						#print "Directory", self.path(), "has a differing entry", name
						self.same_as_base = False
					elif node.code == NODE_SYMLINK_BASED:
						del base_nodes[name]
					else:
						raise "OUCH"
				elif stat.S_ISREG(file_mode):
					node = File(self.backup,self,name)
					node.set_num(ctx.next_num())
					same = node.scan(ctx,cur_prev_nums)
					self.children.append(node)
					self.modified = True
					if node.code == NODE_FILE:
						#print "Directory", self.path(), "has a differing entry", name
						self.same_as_base = False
					elif node.code == NODE_FILE_BASED:
						del base_nodes[name]
					else:
						raise "OUCH"
				elif stat.S_ISDIR(file_mode):
					node = Directory(self.backup,self,name)
					node.set_num(ctx.next_num())
					# The order of append and scan is different here!
					self.children.append(node)
					same = node.scan(ctx,cur_prev_nums)
					if node.code == NODE_DIR:
						#print "Directory", self.path(), "has a differing entry", name
						self.same_as_base = False
					elif node.code == NODE_DIR_BASED:
						del base_nodes[name]
					else:
						raise "OUCH"
				else:
					print "Unecognized file", name
					same = True
			except OSError:
				print "OSError accessing", path
			except IOError:
				print "IOError accessing", path

		if self.same_as_base == True and len(base_nodes) != 0:
			print "Files remained in directory, so it's not the same"
			self.same_as_base = False

		if self.same_as_base==True and self.number != 0 and base_num != None:
			#print "Directory",self.path(),"is same as", base_num, "in base increment"
			# Node 0 must always flush itself
			if ctx.new_files_db.has_key(self.get_key()):
				# this could be added during intermediate flushes
				del ctx.new_files_db[key]
			self.number = base_num
			self.code = NODE_DIR_BASED
		else:
			#print "Directory",self.path(),"is not same as in base increment"
			self.flush(ctx)
			self.code = NODE_DIR

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
			return
		
		valueS = StringIO()
		for child in self.children:
			valueS.write(child.code)
			#if isinstance(child,Directory):
				#valueS.write("D")
			#elif isinstance(child,File):
				#valueS.write("F")
			#elif isinstance(child,Symlink):
				#valueS.write("S")
			#else:
				#raise "Unrecognized object flushing"
			
			self.backup.config.write_int(valueS,child.number)
			self.backup.config.write_string(valueS,child.name)
			
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		self.modified = False
		print "Flushing node", self.path(), "to", key
		
	def restore(self,ctx,based=False):
		if self.path() != ".":
			print "Restoring dir", self.path(), self.number
			os.mkdir(self.path())
		key = self.get_key()
		if self.code == NODE_DIR_BASED or based==True:
			db = ctx.base_files_db
			based = True
		else:
			db = ctx.files_db
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				# no more entries in this dir
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == NODE_DIR or node_type == NODE_DIR_BASED:
				node = Directory(self.backup,self, node_name)
			elif node_type == NODE_FILE or node_type == NODE_FILE_BASED:
				node = File(self.backup,self,node_name)
			elif node_type == NODE_SYMLINK or node_type == NODE_SYMLINK_BASED:
				node = Symlink(self.backup,self,node_name)
			else:
				raise "Unknown node type [%s]"%node_type

			node.set_num(node_num)
			node.code = node_type
			node.restore(ctx,based)

	def request_blocks(self,ctx,block_cache,based=False):
		#print "Requesting blocks in", self.path(), "code", self.code, "num", self.number, "based", based
		if self.code == NODE_DIR_BASED or based==True:
			db = ctx.base_files_db
			based = True
		else:
			db = ctx.files_db
			
		key = self.get_key()
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == NODE_DIR or node_type == NODE_DIR_BASED:
				node = Directory(self.backup,self,node_name)
			elif node_type == NODE_FILE or node_type == NODE_FILE_BASED:
				node = File(self.backup,self,node_name)
			elif node_type == NODE_SYMLINK or node_type == NODE_SYMLINK_BASED:
				# Nothing to do for symlinks
				continue
			else:
				raise "Unknown node type [%s]"%node_type
			node.set_num(node_num)
			node.code = node_type
			node.request_blocks(ctx,block_cache,based)

	def list_files(self,ctx,based=False):
		if self.code == NODE_DIR_BASED or based==True:
			print "B",
			db = ctx.base_files_db
			based = True
		else:
			db = ctx.files_db
			
		print self.path()
		key = self.get_key()
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = self.backup.config.read_int(valueS)
			node_name = self.backup.config.read_string(valueS)
			if node_type == NODE_DIR or node_type == NODE_DIR_BASED:
				node = Directory(self.backup,self,node_name)
			elif node_type == NODE_FILE or node_type == NODE_FILE_BASED:
				node = File(self.backup,self,node_name)
			elif node_type == NODE_SYMLINK or node_type == NODE_SYMLINK_BASED:
				node = Symlink(self.backup,self,node_name)
			node.set_num(node_num)
			node.code = node_type
			#print "Child node", node_name, "code:", node_type
			node.list_files(ctx,based)
