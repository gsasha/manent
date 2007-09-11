import unittest
import random
import time
from cStringIO import StringIO
import stat

# manent imports
import manent.utils.IntegerEncodings as IE
from manent.Nodes import *
from manent.IncrementTree import *

# test util imports
from UtilFilesystemCreator import *

class MockContainerConfig:
	def blockSize(self):
		return 32

class MockGlobalConfig:
	def excludes(self):
		return []

class MockIncrementFSCtx:
	def __init__(self,backup):
		self.backup = backup
	#
	# Funcionality for scanning
	#
	def get_db_level(self,idx):
		# always say that the new db is not a new base
		return self.db_level[idx]
	def is_db_base(self,idx):
		return idx in self.bases
	def is_db_finalized(self,idx):
		return self.backup.is_increment_finalized(idx)
	def get_files_db(self,idx):
		return self.backup.files_db[idx]
	def get_stats_db(self,idx):
		return self.backup.stats_db[idx]

class MockBlockCtx:
	def __init__(self,backup):
		self.backup = backup
	def add_block(self,digest,data):
		self.backup.add_block(digest,data)
	def load_block(self,digest):
		return self.backup.load_block(digest)

class MockHlinkCtx:
	def __init__(self):
		self.inodes_db = {}
	
class MockNumberCtx:
	def __init__(self):
		self.current_num = 0
	def next_number(self):
		self.current_num += 1
		return self.current_num
	
class MockScanCtx(MockIncrementFSCtx,MockBlockCtx,MockHlinkCtx,MockNumberCtx):
	def __init__(self,backup):
		MockIncrementFSCtx.__init__(self,backup)
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)
		MockNumberCtx.__init__(self)
		self.total_nodes = 0
		self.changed_nodes = 0

class MockRestoreCtx(MockIncrementFSCtx,MockBlockCtx,MockHlinkCtx):
	def __init__(self,backup):
		MockIncrementFSCtx.__init__(self,backup)
		MockBlockCtx.__init__(self,backup)
		MockHlinkCtx.__init__(self)

class MockBackup:
	def __init__(self):
		self.container_config = MockContainerConfig()
		self.global_config = MockGlobalConfig()
		self.config_db = {}
		self.blocks_db = {}
		self.files_db = {}
		self.stats_db = {}
		self.increments = IncrementTree(self.config_db)
	def start_increment(self,comment):
		increment = self.increments.start_increment(comment)
		ctx = MockScanCtx(self)
		ctx.new_files_db = {}
		ctx.new_stats_db = {}
		self.files_db[increment.idx] = ctx.new_files_db
		self.stats_db[increment.idx] = ctx.new_stats_db
		ctx.bases = increment.bases
		ctx.scan_bases = increment.scan_bases
		return ctx
	def finalize_increment(self,percent_change):
		class Handler:
			def __init__(self,backup):
				self.backup = backup
			def remove_increment(self,idx):
				del self.files_db[idx]
				del self.stats_db[idx]
			def rebase_fs(self,idx):
				print "rebasing something to ", idx
		self.increments.finalize_increment(percent_change,Handler(self))
	def start_restore(self,idx):
		ctx = MockRestoreCtx(self)
		return ctx
	def is_increment_finalized(self,idx):
		return self.increments.is_increment_finalized(idx)
	def add_block(self,digest,data):
		self.blocks_db[digest] = data
	def load_block(self,digest):
		return self.blocks_db[digest]


class TestNodes(unittest.TestCase):
	def setUp(self):
		self.fsc = FilesystemCreator("/tmp/manent.test.scratch.nodes")
	def tearDown(self):
		pass

	def test_path(self):
		"""Test that path is computed correctly"""
		backup = MockBackup()
		n1 = Node(backup, None, "kuku")
		n2 = Node(backup, n1, "bebe")
		n3 = Node(backup, n2, "mumu.txt")
		self.assertEquals(n1.path(), "kuku")
		self.assertEquals(n2.path(), os.path.join("kuku","bebe"))
		self.assertEquals(n3.path(), os.path.join("kuku","bebe","mumu.txt"))
	def test_hlink(self):
		"""Test that hard links are correctly identified and restored"""
		backup = MockBackup()
		#
		# Create two files linking to the same inode
		#
		ctx = backup.start_increment("")
		self.fsc.add_files({"file1":""})
		self.fsc.link("file1","file2")
		
		root_node = Directory(backup, None, self.fsc.get_home())
		file1_node = File(backup, root_node, "file1")
		file1_node.set_number(ctx.next_number())
		file2_node = File(backup, root_node, "file2")
		file2_node.set_number(ctx.next_number())
		file1_stat = file1_node.stat()
		file2_stat = file2_node.stat()
		#
		# Scan the files...
		#
		self.assertEquals(file1_node.scan_hlink(ctx), False)
		self.assertEquals(file2_node.scan_hlink(ctx), True)
		backup.finalize_increment(1.0)
		#
		# Test that restore works...
		#
		ctx = backup.start_restore(0)
		self.fsc.remove_files({"file2":""})
		self.assertEquals(file1_node.restore_hlink(ctx,file1_stat), False)
		self.assertEquals(file2_node.restore_hlink(ctx,file2_stat), True)
		#
		# Test that the linked file exists and that it is a hard link
		#
		self.assertEquals(self.fsc.test_link("file1","file2"),True)
	def test_File_scan_restore(self):
		"""Test that scanning and restoring a single file works
		We test restoration of both file data and attributes
		"""
		backup = MockBackup()
		ctx = backup.start_increment("for restoring")

		#
		# Scan the files
		#
		file_data = {"file1":"kuku", "file2":""}
		self.fsc.cleanup()
		self.fsc.add_files(file_data)
		self.fsc.chmod("file1",stat.S_IREAD|stat.S_IRWXO)
		self.fsc.chmod("file2",stat.S_IRWXU|stat.S_IRGRP)
		time.sleep(1.1)
		stat1 = self.fsc.lstat("file1")
		stat2 = self.fsc.lstat("file2")

		basedir = Directory(backup, None, self.fsc.get_home())
		node1 = File(backup,basedir,"file1")
		node1.set_level(0)
		node1.set_number(ctx.next_number())
		node1.scan(ctx,[])
		number1 = node1.number
		node2 = File(backup,basedir,"file2")
		node2.set_level(0)
		node2.set_number(ctx.next_number())
		node2.scan(ctx,[])
		number2 = node2.number
		#
		# Restore the files and see if everything is in place
		#
		self.fsc.cleanup()
		restore_node = Directory(backup, None, self.fsc.get_home())
		node1 = File(backup,restore_node,"file1")
		node1.set_level(0)
		node1.set_number(number1)
		node1.restore(ctx)
		node2 = File(backup,restore_node,"file2")
		node2.set_level(0)
		node2.set_number(number2)
		node2.restore(ctx)

		self.failUnless(self.fsc.test_lstat("file1",stat1))
		self.failUnless(self.fsc.test_lstat("file2",stat2))
		# test_files will change access times, do it only after test_lstat
		self.failUnless(self.fsc.test_files(file_data))
	def test_scan_prev_0(self):
		"""
		Test that increments are created at all
		"""
		backup = MockBackup()
		ctx = backup.start_increment("stam1")
		self.fsc.add_files({"file1":"file1_contents", "file2":"file2_contents"})
		root_node = Directory(backup,None,self.fsc.get_home())
		root_node.set_level(0)
		root_node.set_number(ctx.next_number())
		root_node.scan(ctx,[])
		backup.finalize_increment(1.0)
	def prepare_test_scan_prev(self):
		# Prepare the context
		backup = MockBackup()
		ctx = backup.start_increment("")
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
	
