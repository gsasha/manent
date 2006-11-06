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
	def __init__(self,parent,name):
		self.name = name
		self.parent = parent
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
	#def renumber(self,number,node_map):
		#self.number = number
		#return self.number+1

	def get_key(self,backup,number):
		keyS = StringIO()
		keyS.write("N"+str(number))
		#backup.config.write_int(keyS,number)
		return keyS.getvalue()

#--------------------------------------------------------
# CLASS:File
#--------------------------------------------------------
class File(Node):
	def __init__(self,parent,name):
		Node.__init__(self,parent,name)
	#
	# Scanning and restoring
	#
	def scan(self,backup,num,prev_nums):
		print "scanning", self.path()
		#
		# Check if we have seen this file already
		#
		file_stat = os.stat(self.path())
		file_stat_str = " ".join(str(x) for x in file_stat)
		inode_num = file_stat[stat.ST_INO]
		nlink = file_stat[stat.ST_NLINK]
		if nlink > 1:
			if inode_num in backup.inodes_db:
				self.number = backup.inodes_db[inode_num]
				print "  is a hard link to file", self.number
				return num
			# Although file is apparently a hard link, we've not seen it yet
			backup.inodes_db[inode_num] = num
		
		self.number = num
		num += 1
		
		#
		# See if file was in previous increments
		#
		for (db_num,file_num) in prev_nums:
			# TODO: this must work only if the file is old enough
			print "  found in previous increment",
			
			old_db = backup.prev_files_dbs[db_num]
			old_key = self.get_key(backup,file_num)
			old_stat_str = old_db["S"+old_key]
			old_stat = [int(s) for s in re.split("\s+",old_stat_str)]
			if (file_stat[stat.ST_INO]==old_stat[stat.ST_INO]) and \
			            (file_stat[stat.ST_MTIME]==old_stat[stat.ST_MTIME]):
				key = self.get_key(backup,self.number)
				backup.new_files_db[key] = old_db[old_key]
				backup.new_files_db["S"+key] = old_db["S"+old_key]
				print "mtime", file_stat[stat.ST_MTIME], "reusing"
				return num
			else:
				print "but stamp differs:", file_stat_str, "!=", old_stat_str
			
		#
		# File not yet in database, process it
		#
		digests = []
		offset = 0
		read_handle = open(self.path(), "rb")
		while True:
			data = read_handle.read(backup.config.blockSize())
			if len(data)==0:
				break
			digest = backup.config.dataDigest(data)
			digests.append(digest)
			backup.add_block(data,digest)
			#print "Storing block %d of %s, [%d:%d] digest:%s" % (index, self.path(), offset,len(data), base64.b64encode(digest))
			offset += len(data)
		#
		# Serialize to the filesystem db
		#
		key = self.get_key(backup,self.number)
		valueS = StringIO()
		if nlink>1:
			valueS.write("H")
		else:
			valueS.write("F")
		for digest in digests:
			valueS.write(digest)
		backup.new_files_db[key] = valueS.getvalue()
		backup.new_files_db["S"+key] = file_stat_str
		return num
	def restore(self,backup,num):
		key = self.get_key(backup, num)
		valueS = StringIO(backup.files_db[key])
		linktype = valueS.read(1)
		#
		# Check if this file is a hard link to already
		# existing one
		#
		if linktype=="H":
			if num in backup.inodes_db:
				otherFile = backup.inodes_db[num]
				print "Restoring hard link from", otherFile, "to", self.path()
				os.link(otherFile, self.path())
				return
			backup.inodes_db[num] = self.path()

		#
		# No, this file is new. Create it.
		#
		digests = []
		digestSize = backup.config.dataDigestSize()
		while True:
			digest = valueS.read(digestSize)
			if digest=='':
				break
			digests.append(digest)
		
		print "Restoring file", self.path()
		file = open(self.path(), "w")
		for digest in digests:
			#print "File", self.path(), "reading digest", base64.b64encode(digest)
			file.write(backup.read_block(digest))
	def request_blocks(self,backup,num,block_cache):
		key = self.get_key(backup,num)
		valueS = StringIO(backup.files_db[key])
		linktype = valueS.read(1)

		if linktype=="H":
			if num in backup.inodes_db:
				# This file is a hard link, so it needs no blocks
				return
			backup.inodes_db[num] = self.path()
		#
		# Ok, this file is new. Count all its blocks
		#
		digestSize = backup.config.dataDigestSize()
		while True:
			digest = valueS.read(digestSize)
			if digest == "": break
			block_cache.request_block(digest)
	def list_files(self,backup,num,db):
		print self.path()
#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
	def __init__(self,parent,name):
		Node.__init__(self,parent,name)
	def scan(self,backup,num,prev_nums):
		self.link = os.readlink(self.path())
		print "scanning", self.path(), "->", self.link
		key = self.get_key(backup,num)
		backup.new_files_db[key] = self.link
		self.number = num
		return num+1
	def restore(self,backup,num):
		key = self.get_key(backup,num)
		self.link = backup.files_db[key]
		print "Restoring symlink from", self.link, "to", self.path()
		os.symlink(self.link, self.path())
	def list_files(self,backup,num,db):
		print self.path()

##--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
	def __init__(self,parent,name):
		Node.__init__(self,parent,name)
	def scan(self,backup,num,prev_nums):
		self.number = num
		num = num+1
		#
		# Reload data from previous increments
		#
		print "Path", self.path(), "found in previous increments:", prev_nums
		prev_data = {}
		for (db_num,file_num) in prev_nums:
			db = backup.prev_files_dbs[db_num]
			file_key = self.get_key(backup,file_num)
			if not db.has_key(file_key):
				print "DB %d doesn't have key %s" % (db_num,file_key)
				continue
			valueS = StringIO(db[file_key])
			while True:
				node_type = valueS.read(1)
				if node_type == "":
					break
				node_num = backup.config.read_int(valueS)
				node_name = backup.config.read_string(valueS)
				if not prev_data.has_key(node_name):
					prev_data[node_name] = []
				prev_data[node_name].append((db_num,node_num))
				#print node_name,
			#print
		#print "Prev data:"
		#keys = prev_data.keys()
		#keys.sort()
		#for key in keys:
			#print " ", key, prev_data[key]
		#print "Prev files:", prev_data
		#
		# Scan the directory
		#
		key = self.get_key(backup,self.number)
		# first 20 files in the directory are written immediately!
		next_flush = 20
		children = []
		for file in os.listdir(self.path()):
			if (file=="..") or (file=="."):
				continue
			name = os.path.join(self.path(),file)
			file_mode = os.stat(name)[stat.ST_MODE]
			cur_prev_nums = []
			if prev_data.has_key(file):
				cur_prev_nums = prev_data[file]
			try:
				if os.path.islink(name):
				#if stat.S_ISLNK(file_mode):
					node = Symlink(self, file)
					num = node.scan(backup,num,cur_prev_nums)
					children.append((node.number,"S",file))
				elif stat.S_ISREG(file_mode):
					node = File(self,file)
					num = node.scan(backup,num,cur_prev_nums)
					children.append((node.number,"F",file))
				elif stat.S_ISDIR(file_mode):
					node = Directory(self,file)
					# We know that this is the number that the node
					# will get anyway - assign it without scanning
					#num = node.scan(backup,num,cur_prev_nums)
					children.append((num,"D",file))
				
				if len(children)<20 or len(children) > next_flush:
					if len(children)>next_flush:
						next_flush = next_flush*2
					#print "$$$$$$$$ Saving intermediate version of", self.path()
					valueS = StringIO()
					for (node_num,node_type,node_name) in children:
						valueS.write(node_type)
						backup.config.write_int(valueS,node_num)
						backup.config.write_string(valueS,node_name)
					backup.new_files_db[key] = valueS.getvalue()
				
				if stat.S_ISDIR(file_mode):
					num = node.scan(backup,num,cur_prev_nums)
			except OSError:
				print "OSError accessing", name
			except IOError:
				print "IOError accessing", name
				
		#print "$$$$$$$$ Saving final version of", self.path()
		valueS = StringIO()
		for (node_num,node_type,node_name) in children:
			valueS.write(node_type)
			backup.config.write_int(valueS,node_num)
			backup.config.write_string(valueS,node_name)
		backup.new_files_db[key] = valueS.getvalue()
		#print "files_db[%s] = %s" % (key,valueS.getvalue())
		return num
	def restore(self,backup,num):
		if self.path() != ".":
			print "Restoring dir", self.path(), num
			os.mkdir(self.path())
		children = []
		key = self.get_key(backup,num)
		valueS = StringIO(backup.files_db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				# no more entries in this dir
				break
			node_num = backup.config.read_int(valueS)
			node_name = backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self, node_name)
			elif node_type == "F":
				node = File(self,node_name)
			elif node_type == "S":
				node = Symlink(self,node_name)
			else:
				raise "Unknown node type [%s]"%node_type
			
			node.restore(backup,node_num)

	def request_blocks(self,backup,num,block_cache):
		key = self.get_key(backup,num)
		valueS = StringIO(backup.files_db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = backup.config.read_int(valueS)
			node_name = backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self,node_name)
			elif node_type == "F":
				node = File(self,node_name)
			elif node_type == "S":
				# Nothing to do for symlinks
				continue
			else:
				raise "Unknown node type [%s]"%node_type
			node.request_blocks(backup,node_num,block_cache)

	def list_files(self,backup,num,db):
		print self.path()
		key = self.get_key(backup,num)
		valueS = StringIO(db[key])
		while True:
			node_type = valueS.read(1)
			if node_type == "":
				break
			node_num = backup.config.read_int(valueS)
			node_name = backup.config.read_string(valueS)
			if node_type == "D":
				node = Directory(self, node_name)
			elif node_type == "F":
				node = File(self,node_name)
			elif node_type == "S":
				node = Symlink(self,node_name)
			node.list_files(backup,node_num,db)
