import os, os.path, stat
import base64
from cStringIO import StringIO
import re
import manent.utils.IntegerEncodings as IntegerEncodings
import manent.utils.Digest as Digest
import manent.utils.Format as Format
from manent.utils.FileIO import read_blocks
import traceback

import Backup

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
	#
	# Scanning and restoring
	#
	def scan(self,ctx,prev_nums):
		print "scanning", self.path(), prev_nums
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
				print inode_num,ctx.inodes_db[inode_num]
				(self.number,self.code) = ctx.inodes_db[inode_num]
				print "  is a hard link to file", self.number
				return
		
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
				# This file was previously of different type, i.e.,
				# it could be a directory and now a symlink.
				# That's fine, but we can't of course base on it anymore
				break

			# load the old data
			old_key = self.node_key(file_num)
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
					#
					# We have used old_db, and created new one out of it.
					# Old one is no longer needed
					#
					del old_db[old_key]
					del old_db["S"+old_key]
					
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
		for data in read_blocks(open(self.path(), "rb"), self.backup.container_config.blockSize()):
			digest = Digest.dataDigest(data)
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
				otherFile = ctx.inodes_db[self.number]
				print "Restoring hard link from", otherFile, "to", self.path()
				os.link(otherFile, self.path())
				return
			ctx.inodes_db[self.number] = self.path()

		#
		# No, this file is new. Create it.
		#
		print "Restoring file", self.path()
		file = open(self.path(), "wb")
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(self.backup.blocks_cache.load_block(digest))
	
	def request_blocks(self,ctx,block_cache,based):
		if self.code == NODE_FILE_BASED or based==True:
			db = ctx.base_files_db
		else:
			db = ctx.files_db
		key = self.get_key()
		valueS = StringIO(db[key])
		
		linktype = valueS.read(1)

		if linktype=="H":
			if ctx.inodes_db.has_key(self.number):
				# This file is a hard link, so it needs no blocks
				return
			ctx.inodes_db[self.number] = self.path()
		#
		# Ok, this file is new. Count all its blocks
		#
		for digest in read_blocks(valueS,Digest.dataDigestSize()):
			block_cache.request_block(digest)
	def list_files(self,ctx,based):
		if self.code == NODE_FILE_BASED or based == True:
			print "B",
		print self.path(), self.number

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
				# This file was previously of different type, i.e.,
				# it could be a directory and now a symlink.
				# That's fine, but we can't of course base on it anymore
				break

			if old_db[self.node_key(file_num)] == self.link:
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
		print self.path(), self.number

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
		# prev_data keeps for each file in this directory the list of backups
		# in which it was found
		prev_data = {}
		# base_nodes keeps the information on which nodes have been existing
		# in the base version of this directory, in order to decide whether
		# the directory has changed
		base_nodes = {}
		# base_num will contain the number of this node in the base directory
		base_num = None
		for (db_num,file_num,file_code) in prev_nums:
			if file_code == NODE_DIR_BASED:
				base_num = file_num
			
			db = ctx.prev_files_dbs[db_num]
			file_key = self.node_key(file_num)
			if not db.has_key(file_key):
				if ctx.base_files_db == None:
					print "DB %d doesn't have key [%s]" % (db_num,base64.b64encode(file_key))
					continue
				base_key = self.node_key(base_num)
				valueS = StringIO(ctx.base_files_db[base_key])
			else:
				valueS = StringIO(db[file_key])
			while True:
				node_type = valueS.read(1)
				if node_type == "":
					break
				node_num = Format.read_int(valueS)
				node_name = Format.read_string(valueS)

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

		excluded = False
		for checker in self.backup.global_config.excludes():
			if checker(self.path()):
				entries = []
				print "Excluding directory from scan:", self.path()
				break
		else:
			entries = os.listdir(self.path())
		#if self.path().startswith(self.backup.global_config.home_area()):
			#entries = []
		#else:
			#entries = os.listdir(self.path())
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
				traceback.print_exc()
			except IOError, (errno, strerror):
				print "IOError %s accessing '%s'" % (errno,strerror), path
				traceback.print_exc()

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
			
			Format.write_int(valueS,child.number)
			Format.write_string(valueS,child.name)
			
		key = self.get_key()
		ctx.new_files_db[key] = valueS.getvalue()
		self.modified = False
		#print "Flushing node", self.path(), "to", base64.b64encode(key)
		
	def restore(self,ctx,based=False):
		if self.parent != None:
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
			node_num = Format.read_int(valueS)
			node_name = Format.read_string(valueS)
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
			node_num = Format.read_int(valueS)
			node_name = Format.read_string(valueS)
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
			
		print self.path(), self.number
		key = self.get_key()
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = Format.read_int(valueS)
			node_name = Format.read_string(valueS)
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
