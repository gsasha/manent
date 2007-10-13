import unittest
import random
import time
from cStringIO import StringIO
import stat
import base64

# manent imports
import manent.utils.IntegerEncodings as IE
from manent.Nodes import *

# test util imports
from UtilFilesystemCreator import *
from Mock import *

#TODO:
# Test directories
# Test symlinks
# Test that prev information is read correctly

class TestNodes(unittest.TestCase):
	def setUp(self):
		self.fsc = FilesystemCreator("/tmp/manent.test.scratch.nodes")
	def tearDown(self):
		pass

	def test_path(self):
		"""Test that path is computed correctly"""
		backup = MockBackup(self.fsc.get_home())
		n1 = Node(backup, None, "kuku")
		n2 = Node(backup, n1, "bebe")
		n3 = Node(backup, n2, "mumu.txt")
		self.assertEquals(n1.path(), "kuku")
		self.assertEquals(n2.path(), os.path.join("kuku","bebe"))
		self.assertEquals(n3.path(), os.path.join("kuku","bebe","mumu.txt"))
	def test_hlink(self):
		"""Test that hard links are correctly identified and restored"""
		backup = MockBackup(self.fsc.get_home())
		#
		# Create two files linking to the same inode
		#
		ctx = backup.start_increment("")
		self.fsc.add_files({"file1":""})
		self.fsc.link("file1","file2")
		
		root_node = Directory(backup, None, self.fsc.get_home())
		file1_node = File(backup, root_node, "file1")
		file2_node = File(backup, root_node, "file2")
		file1_node.compute_stats()
		file2_node.compute_stats()
		file1_stat = file1_node.get_stats()
		file2_stat = file2_node.get_stats()
		#
		# Scan the files...
		#
		self.assertEquals(file1_node.scan_hlink(ctx), False)
		file1_node.compute_stats()
		file1_node.scan(ctx,[])
		file1_node.update_hlink(ctx)
		self.assertEquals(file2_node.scan_hlink(ctx), True)
		file2_node.compute_stats()
		file2_node.scan(ctx,[])
		file2_node.update_hlink(ctx)
		backup.finalize_increment()
		#
		# Test that restore works...
		#
		self.fsc.reset()
		ctx = backup.start_restore(0)
		#self.assertEquals(file1_node.restore_hlink(ctx,file1_stat), False)
		file1_node.restore(ctx)
		self.assertEquals(file2_node.restore_hlink(ctx), True)
		#
		# Test that the linked file exists and that it is a hard link
		#
		self.assertEquals(self.fsc.test_link("file1","file2"),True)
	def test_File_scan_restore(self):
		"""Test that scanning and restoring a single file works
		We test restoration of both file data and attributes
		"""
		backup = MockBackup(self.fsc.get_home())
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
		node1.compute_stats()
		node1.scan(ctx,[])
		digest1 = node1.get_digest()
		stats1 = node1.get_stats()
		node2 = File(backup,basedir,"file2")
		node2.compute_stats()
		node2.scan(ctx,[])
		digest2 = node2.get_digest()
		stats2 = node2.get_stats()
		#
		# Restore the files and see if everything is in place
		#
		self.fsc.reset()
		restore_node = Directory(backup, None, self.fsc.get_home())
		node1 = File(backup,restore_node,"file1")
		node1.set_digest(digest1)
		node1.set_stats(stats1)
		node1.restore(ctx)
		node2 = File(backup,restore_node,"file2")
		node2.set_digest(digest2)
		node2.set_stats(stats2)
		node2.restore(ctx)

		self.failUnless(self.fsc.test_lstat("file1",stat1))
		self.failUnless(self.fsc.test_lstat("file2",stat2))
		# test_files will change access times, do it only after test_lstat
		self.failUnless(self.fsc.test_files(file_data))
	
	def test_symlink(self):
		"""Test that symlinks and hard links are scanned and restored correctly"""
		backup = MockBackup(self.fsc.get_home())
		ctx = backup.start_increment("for restoring")

		#
		# Scan the files
		#
		f1 = FSCFile("kuku")
		f2 = FSCSymlink("file1")
		file_data = {"file1":f1, "file2":f2, "file3":f1, "file4":f2}
		self.fsc.reset()
		self.fsc.add_files(file_data)


		# Scan the directory structure
		basedir = Directory(backup, None, self.fsc.get_home())
		basedir.scan(ctx,[])
		digest = basedir.get_digest()
	def test_directory(self):
		"""Test that directories are scanned and restored correctly"""
		backup = MockBackup(self.fsc.get_home())
		ctx = backup.start_increment("for restoring")

		# Create the directory structure
		file_data = {"file1":FSCFile("kuku"), "dir1": {"file2":FSCFile("kuku"),"file3":FSCFile("bebe")}}
		self.fsc.reset()
		self.fsc.add_files(file_data)
		self.failUnless(self.fsc.test_files(file_data))

		# Scan the directory structure
		basedir = Directory(backup, None, self.fsc.get_home())
		basedir.scan(ctx,[])
		digest = basedir.get_digest()

		# Try to restore
		self.fsc.reset()
		restore_dir = Directory(backup, None, self.fsc.get_home())
		restore_dir.set_digest(digest)
		restore_dir.restore(ctx)

		self.failUnless(self.fsc.test_files(file_data))
	def test_prev(self):
		"""Test that the information from previous versions is taken into account"""
		pass
