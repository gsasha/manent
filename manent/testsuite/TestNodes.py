import unittest
import random
import os, os.path, shutil
import stat
import time
from cStringIO import StringIO

import manent.utils.IntegerEncodings as IE
from manent.Nodes import *
from manent.IncrementTree import *

class MockContainerConfig:
	def blockSize(self):
		return 32
class MockBackup:
	def __init__(self):
		self.container_config = MockContainerConfig()
		self.blocks = {}
		self.shared_db = {}
		self.increments = IncrementTree(self.shared_db)
	def add_block(self,digest,data):
		self.blocks[digest] = data

class TestFilesystemCreator:
	def __init__(self,home):
		self.home = home
		try:
			shutil.rmtree(self.scratch)
		except:
			# If we run for the first time, the dir doesn't exists
			pass
		os.mkdir(self.home)

	def get_home(self):
		return self.home
	
	def add_files(self,files):
		self.__add_files(self.home,files)
	def __add_files(self,prefix,files):
		for name,contents in files:
			path = os.path.join(prefix,name)
			if type(contents) == type({}):
				try:
					os.mkdir(path)
				except:
					pass
				self.__add_files(path,contents)
			else:
				f = open(path,"w")
				f.write(contents)
				f.close()
	def __remove_files(self,prefix,files):
		self.__remove_files(self.home,files)
	def __remove_files(self,prefix,files):
		for name,contents in files:
			path = os.path.join(prefix,name)
			if type(contents) == type({}):
				self.__add_files(path,contents)
				if len(os.listdir(path))==0:
					os.rmdir(path)
			else:
				os.unlink(path)
	def link(self,file1,file2):
		if file1.startswith("/"):
			os.link(file1,os.path.join(self.home,file2))
		else:
			os.link(os.path.join(self.home,file1),os.path.join(self.home,file2))
	def symlink(self,file1,file2):
		os.symlink(file1,os.path.join(self.home,file2))
	def symlink_absolute(self,file1,file2):
		if file1.startswith("/"):
			os.symlink(file1,os.path.join(self.home,file2))
		else:
			os.symlink(os.path.join(self.home,file1),os.path.join(self.home,file2))

class MockIncrementFSCtx:
	def __init__(self):
		pass
	# Funcionality for scanning
	def get_db_level(self,idx):
		# always say that the new db is not a new base
		return self.db_level[idx]
	def is_db_base(self,idx):
		return self.is_base(idx)
	def is_db_finalized(self,idx):
		return self.is_finalized(idx)
	def get_files_db(self,idx):
		return self.files_db[idx]
	def get_stats_db(self,idx):
		return self.stats_db[idx]
class MockCtx(MockIncrementFSCtx):
	def __init__(self,backup):
		MockIncrementFSCtx.__init__(self)
		self.backup = backup
		self.inodes_db = {}
		self.current_num = 0
		self.total_nodes = 0
		self.changed_nodes = 0
	def next_num(self):
		self.current_num += 1
		return self.current_num
	def add_block(self,data,digest):
		self.backup.add_block(digest,data)

class TestNodes(unittest.TestCase):
	def setUp(self):
		self.backup = MockBackup()
		self.fsc = TestFilesystemCreator("/tmp/manent.test.scratch.nodes")
	def tearDown(self):
		pass

	def test_path(self):
		"""Test that path is computed correctly"""
		n1 = Node(self.backup, None, "kuku")
		n2 = Node(self.backup, n1, "bebe")
		n3 = Node(self.backup, n2, "mumu.txt")
		self.assertEquals(n1.path(), "kuku")
		self.assertEquals(n2.path(), os.path.join("kuku","bebe"))
		self.assertEquals(n3.path(), os.path.join("kuku","bebe","mumu.txt"))
	def test_stat(self):
		"""Test stat operations"""
		scratch_node = Directory(self.backup,None,self.fsc.get_home())

		# Test that created single files have correct size and
		# inode count and non-decreasing timestamps
		self.fsc.add_files({"file1":"abcdef", "file2":"aaaaa"})
		file_node = File(self.backup, scratch_node, "file1")
		file_stat = file1_node.stat()
		self.assertEquals(file1_stat[stat.ST_NLINK], 1)
		self.assertEquals(file1_stat[stat.ST_SIZE], 6)

		file2_node = File(self.backup, scratch_node, "file2")
		file2_stat = file2_node.stat()
		self.assertEquals(file2_stat[stat.ST_NLINK], 1)
		self.assertEquals(file2_stat[stat.ST_SIZE], 5)
		
		self.assertEquals(file1_stat[stat.ST_UID], file2_stat[stat.ST_UID])
		self.assertEquals(file1_stat[stat.ST_GID], file2_stat[stat.ST_GID])
		self.failUnless(file1_stat[stat.ST_ATIME] <= file2_stat[stat.ST_ATIME])

		# Test that if we create a hard link, link count goes up
		self.fsc.link("file1","hardlinked")
		# need to recreate the node to avoid seeing cached results
		file1_node = File(self.backup, scratch_node, self.file1_name)
		file1_stat = file1_node.stat()
		self.assertEquals(file1_stat[stat.ST_NLINK], 2)

		# Test that hardlinking works and is reported for symlinks
		self.fsc.symlink("file1","file1.lnk")
		file1_lnk_node = Symlink(self.backup, scratch_node, "file1.lnk")
		file1_lnk_stat = file1_lnk_node.stat()
		self.assertEquals(file1_lnk_stat[stat.ST_NLINK],1)

		os.link(os.path.join(self.scratch, "file1.lnk"), os.path.join(self.scratch,"file2.lnk"))
	def test_hlink(self):
		scratch_node = Directory(self.backup, None, self.scratch)
		#
		# Create two files lining to the same inode
		#
		ctx = MockCtx(self.backup)
		self.fsc.add_files({"file1",""})
		self.fsc.link("file1","file2")
		
		file1_node = File(self.backup, scratch_node, "file1")
		file1_node.set_num(ctx.next_num())
		file2_node = File(self.backup, scratch_node, "file2")
		file2_node.set_num(ctx.next_num())
		file1_stat = file1_node.stat()
		file2_stat = file2_node.stat()
		#
		# Scan the files...
		#
		self.assertEquals(file1_node.scan_hlink(ctx), False)
		self.assertEquals(file2_node.scan_hlink(ctx), True)
		#
		# Test that restore works...
		#
		ctx = MockCtx(self.backup)
		os.unlink(self.file2_path)
		self.assertEquals(file1_node.restore_hlink(ctx,file1_stat), False)
		self.assertEquals(file2_node.restore_hlink(ctx,file2_stat), True)
		#
		# Test that the linked file exists and that it is a hard link
		#
		tmp_node = File(self.backup,scratch_node, self.file2_name)
		self.assertEquals(tmp_node.stat()[stat.ST_NLINK], 2)
	def test_scan_prev_0(self):
		"""
		Test that increments are created at all
		"""
		ctx = MockCtx(self.backup)
		self.backup.start_increment()
		self.fsc.add_files({"file1":"file1_contents", "file2":"file2_contents"})
		root_node = Directory(self.backup,self.fsc.get_home())
		root_node.scan()
		self.backup.finalize_increment()
	def prepare_test_scan_prev(self):
		# Prepare the context
		ctx = MockCtx(self.backup)
		ctx.base_fs = {0:0,1:5}
		(f1,f2,f3) = ({},{},{})
		(s1,s2,s3) = ({},{},{})
		ctx.open_files_dbs = {0:f1,5:f2,6:f3}
		ctx.open_stats_dbs = {0:s1,5:s2,6:s3}
		ctx.finalized = {0:True,5:False,6:False}
		#
		# Create the files and scan them into the files_dbs...
		#
		self.fsc.add_files({"file1":"file1 contents","file2":"file2 contents"})

		scratch_node = Directory(self.backup,None, self.scratch)
		file1_node = File(self.backup, scratch_node, "file1")
		file1_node.set_num(ctx.next_num())
		file2_node = File(self.backup, scratch_node, "file2")
		file2_node.set_num(ctx.next_num())

		# Put file1 in base increment 0 (level 0)
		ctx.new_files_db = ctx.open_files_dbs[0]
		ctx.new_stats_db = ctx.open_stats_dbs[0]
		file1_node.scan(ctx,[])
		# Put file2 in base increment 5 (level 1)
		ctx.new_files_db = ctx.open_files_dbs[5]
		ctx.new_stats_db = ctx.open_stats_dbs[5]
		file2_node.scan(ctx,[])

		prev_nums_1 = [(0,file1_node.number,node_encode(NODE_TYPE_FILE,0)),
		               (5,file1_node.number,node_encode(NODE_TYPE_FILE,None))]
		prev_nums_2 = [(0,file2_node.number,node_encode(NODE_TYPE_FILE,0)),
		               (5,file2_node.number,node_encode(NODE_TYPE_FILE,None))]
		
		return (ctx,file1_node,file2_node,prev_nums_1,prev_nums_2)
	def test_scan_prev_1(self):
		(ctx,file1_node,file2_node,prev_nums_1,prev_nums_2) = self.prepare_test_scan_prev()
		ctx.new_files_db = ctx.open_files_dbs[6]
		ctx.new_stats_db = ctx.open_stats_dbs[6]
		# We don't give the increments as bases, so we shouldn't find them
		prev_nums = []
		self.assertEquals(file1_node.scan_prev(ctx,prev_nums),False)
		self.assertEquals(file2_node.scan_prev(ctx,prev_nums),False)
	
	def test_scan_prev_2(self):
		(ctx,file1_node,file2_node,prev_nums_1,prev_nums_2) = self.prepare_test_scan_prev()
		ctx.new_files_db = ctx.open_files_dbs[6]
		ctx.new_stats_db = ctx.open_stats_dbs[6]
		# If we give the increments containing the previous nodes,
		# we should find them
		file1_node.uncache()
		file2_node.uncache()
		time.sleep(1.01)
		self.assertEquals(file1_node.scan_prev(ctx,prev_nums_1),True)
		self.assertEquals(file2_node.scan_prev(ctx,prev_nums_2),True)
		# And make sure that they are deleted from the previous dbs
		self.assertEquals(len(ctx.open_files_dbs[0]), 1)
		self.assertEquals(len(ctx.open_stats_dbs[0]), 1)
		self.assertEquals(len(ctx.open_files_dbs[5]), 0)
		self.assertEquals(len(ctx.open_stats_dbs[5]), 0)

	def test_scan_prev_3(self):
		(ctx,file1_node,file2_node,prev_nums_1,prev_nums_2) = self.prepare_test_scan_prev()
		ctx.new_files_db = ctx.open_files_dbs[6]
		ctx.new_stats_db = ctx.open_stats_dbs[6]
		# We don't give the increments as bases, so we shouldn't find them
		time.sleep(1.0)
		file1 = open(self.file1_path,"r")
		file1.close()
		file2 = open(self.file2_path,"a")
		file2.write("kuku")
		file2.close()
		# File1 was opened for reading, that's OK.
		# File2 was opened for writing, so it cannot be scanned as prev...
		file1_node.uncache()
		file2_node.uncache()
		
		self.assertEquals(file1_node.scan_prev(ctx,prev_nums_1),True)
		self.assertEquals(file2_node.scan_prev(ctx,prev_nums_2),False)

	def test_scan_prev_4(self):
		(ctx,file1_node,file2_node,prev_nums_1,prev_nums_2) = self.prepare_test_scan_prev()
		ctx.new_files_db = ctx.open_files_dbs[6]
		ctx.new_stats_db = ctx.open_stats_dbs[6]
		#time.sleep(0.1)
		file1 = open(self.file1_path,"r")
		file1.close()
		file2 = open(self.file2_path,"a")
		file2.write("kuku")
		file2.close()
		# File1 was opened for reading, but it's too recent, so it cannot be used
		# File2 was opened for writing, so it cannot be scanned as prev...
		file1_node.uncache()
		file2_node.uncache()
		
		self.assertEquals(file1_node.scan_prev(ctx,prev_nums_1),False)
		self.assertEquals(file2_node.scan_prev(ctx,prev_nums_2),False)
	
	def test_scan_prev_5(self):
		(ctx,file1_node,file2_node,prev_nums_1,prev_nums_2) = self.prepare_test_scan_prev()
		ctx.new_files_db = ctx.open_files_dbs[6]
		ctx.new_stats_db = ctx.open_stats_dbs[6]
		time.sleep(1.1)
		# keep the old file to make sure that its inode stays
		# reserved and is not reused!
		os.rename(self.file1_path,self.file1_path+"...old")
		os.rename(self.file2_path,self.file2_path+"...old")
		file1 = open(self.file1_path,"w")
		file1.close()
		os.mkdir(self.file2_path)
		# File1 was recreated, its inode should be different!
		# File2 was converted to a directory
		file1_node.uncache()
		file2_node.uncache()
		
		self.assertEquals(file1_node.scan_prev(ctx,prev_nums_1),False)
		self.assertEquals(file2_node.scan_prev(ctx,prev_nums_2),False)
	
	def test_File_scan_restore(self):
		"""
		Test that scanning and restoring a single file works
		"""
		scratch_node = Directory(self.backup, None, self.scratch)
		ctx = MockCtx(self.backup)
		ctx.new_files_db = {}
		ctx.new_stats_db = {}
		
		file1 = open(self.file1_path,"w")
		file1.write("kuku")
		file1.close()
		file1_node = File(self.backup,scratch_node,self.file1_name)

		file1_node.set_num(ctx.next_num())
		file1_node.scan(ctx,[])
		restore_path = os.path.join(self.scratch,"restore")
		os.mkdir(restore_path)
		restore_node = Directory(self.backup, None, restore_path)
		file2_node = File(self.backup,restore_node,self.file1_name)
		file2_node.set_num(file1_node.number)
		file2_node.restore(ctx,None)

		file2 = open(os.path.join(restore_path,file1_name),"r")
		self.assertEquals(file2.read(), "kuku")
