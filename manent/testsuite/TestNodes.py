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
from Mock import *

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
		self.fsc.reset()
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
		self.fsc.reset()
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
	def test_scan_prev_file(self):
		"""Test that file is found in the history"""
		backup = MockBackup()
		self.fsc.reset()
		#
		# First increment. File1 is there
		#
		self.fsc.add_files({"file1":"file1_contents"})
		ctx = backup.start_increment("")
		root_node = Directory(backup,None,self.fsc.get_home())
		root_node.set_level(ctx.get_level())
		root_node.set_number(ctx.next_number())
		root_node.scan(ctx,[])
		backup.finalize_increment(ctx.get_change_percent())
		level1 = ctx.get_level()
		#
		# Second increment. File2 is there
		#
		self.fsc.add_files({"file2":"file2_contents"})
		ctx = backup.start_increment("")
		root_node = Directory(backup,None,self.fsc.get_home())
		root_node.set_level(ctx.get_level())
		root_node.set_number(ctx.next_number())
		root_node.scan(ctx,[])
		backup.finalize_increment(ctx.get_change_percent())
		level2 = ctx.get_level()
		#
		# Check that base was found for file1 and file2, but not for file3
		#
		self.fsc.add_files({"file3":"file3_contents"})
		ctx = backup.start_increment("")
		root_node = Directory(backup,None,self.fsc.get_home())
		root_node.set_level(ctx.get_level())
		root_node.set_number(ctx.next_number())
		root_node.scan(ctx,[])
		backup.finalize_increment(ctx.get_change_percent())
		level3 = ctx.get_level()
		#
		# Peform the check
		#
		self.assertEquals(root_node.get_node("file1").get_level() == level1)
		self.assertEquals(root_node.get_node("file2").get_level() == level2)
		self.assertEquals(root_node.get_node("file3").get_level() == level3)
