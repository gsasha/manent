#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import os.path
import random
import stat
import sys
import time
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

# manent imports
import manent.utils.IntegerEncodings as IE
import manent.ExclusionProcessor as EP
import manent.Nodes as Nodes

# test util imports
import UtilFilesystemCreator as FSC
import Mock

#TODO:
# Test directories
# Test symlinks
# Test that prev information is read correctly

class TestNodes(unittest.TestCase):
  def setUp(self):
    self.fsc = FSC.FilesystemCreator()
  def tearDown(self):
    #self.fsc.cleanup()
    pass

  def test_path(self):
    # Test that path is computed correctly
    backup = Mock.MockBackup(self.fsc.get_home())
    n1 = Nodes.Node(backup, None, u"kuku")
    n2 = Nodes.Node(backup, n1, u"bebe")
    n3 = Nodes.Node(backup, n2, u"mumu.txt")
    self.assertEquals(n1.path(), u"kuku")
    self.assertEquals(n2.path(), os.path.join(u"kuku", "bebe"))
    self.assertEquals(n3.path(), os.path.join(u"kuku", "bebe", "mumu.txt"))

  def test_unicode_path(self):
    backup = Mock.MockBackup(self.fsc.get_home())
    s1 = unicode('\xd7\x90\xd7\x99\xd7\x9f \xd7\x96\xd7\x95'
          ' \xd7\x90\xd7\x92\xd7\x93\xd7\x94', 'utf8')
    s2 = unicode('\xd0\x9f\xd0\xbe\xd0\xbf\xd1\x8b\xd1\x82\xd0\xba\xd0\xb0'
          '\xd0\xbd\xd0\xb5 \xd0\xbf\xd1\x8b\xd1\x82\xd0\xba\xd0\xb0', 'utf8')
    s3 = unicode('I \xd1\x81\xd1\x80\xd1\x83 \xd7\xa4\xd7\x94 \xc3\xbc\xc3\xb6'
            '\xc3\xa4', 'utf8')
    n1 = Nodes.Node(backup, None, s1)
    n2 = Nodes.Node(backup, n1, s2)
    n3 = Nodes.Node(backup, n2, s3)
    self.assertEquals(n1.path(), s1)
    self.assertEquals(n2.path(), os.path.join(s1, s2))
    self.assertEquals(n3.path(), os.path.join(s1, s2, s3))

  def test_hlink(self):
    # Test that hard links are correctly identified and restored
    if not FSC.supports_hard_links():
      return

    backup = Mock.MockBackup(self.fsc.get_home())
    #
    # Create two files linking to the same inode
    #
    ctx = backup.start_increment("")
    self.fsc.add_files({u"file1": ""})
    self.fsc.link(u"file1", u"file2")
    
    root_node = Nodes.Directory(backup, None, self.fsc.get_home())
    file1_node = Nodes.File(backup, root_node, u"file1")
    file2_node = Nodes.File(backup, root_node, u"file2")
    file1_node.compute_stats()
    file2_node.compute_stats()
    file1_stat = file1_node.get_stats()
    file2_stat = file2_node.get_stats()

    # Hardlinked files should have the same stat.
    self.assertEquals(file1_stat, file2_stat)
    #
    # Scan the files...
    #
    self.assertEquals(file1_node.scan_hlink(ctx), False)
    file1_node.compute_stats()
    file1_node.scan(ctx, None)
    file1_node.update_hlink(ctx)
    self.assertEquals(file2_node.scan_hlink(ctx), True)
    file2_node.compute_stats()
    file2_node.scan(ctx, None)
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
    self.assertEquals(self.fsc.test_link(u"file1", u"file2"), True)
  def test_File_scan_restore(self):
    # Test that scanning and restoring a single file works
    # We test restoration of both file data and attributes
    backup = Mock.MockBackup(self.fsc.get_home())
    ctx = backup.start_increment("for restoring")

    #
    # Scan the files
    #
    file_data = {u"file1" : "kuku",
        u"file2" : ""}
    self.fsc.reset()
    self.fsc.add_files(file_data)
    self.fsc.chmod(u"file1", stat.S_IREAD|stat.S_IRWXO)
    self.fsc.chmod(u"file2", stat.S_IRWXU|stat.S_IRGRP)
    time.sleep(1.1)
    stat1 = self.fsc.lstat(u"file1")
    stat2 = self.fsc.lstat(u"file2")

    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    node1 = Nodes.File(backup, basedir, u"file1")
    node1.compute_stats()
    node1.scan(ctx, None)
    digest1 = node1.get_digest()
    level1 = node1.get_level()
    stats1 = node1.get_stats()
    node2 = Nodes.File(backup, basedir, u"file2")
    node2.compute_stats()
    node2.scan(ctx, None)
    digest2 = node2.get_digest()
    level2 = node2.get_level()
    stats2 = node2.get_stats()
    #
    # Restore the files and see if everything is in place
    #
    self.fsc.reset()
    restore_node = Nodes.Directory(backup, None, self.fsc.get_home())
    node1 = Nodes.File(backup, restore_node, u"file1")
    node1.set_digest(digest1)
    node1.set_level(level1)
    node1.set_stats(stats1)
    node1.restore(ctx)
    node2 = Nodes.File(backup, restore_node, u"file2")
    node2.set_digest(digest2)
    node2.set_level(level2)
    node2.set_stats(stats2)
    node2.restore(ctx)

    self.failUnless(self.fsc.test_lstat(u"file1", stat1))
    self.failUnless(self.fsc.test_lstat(u"file2", stat2))
    # test_files will change access times, do it only after test_lstat
    self.failUnless(self.fsc.test_files(file_data))
  
  def test_symlink(self):
    # Test that symlinks and hard links are scanned and restored correctly
    if not FSC.supports_symbolic_links():
      return

    backup = Mock.MockBackup(self.fsc.get_home())
    ctx = backup.start_increment("for restoring")

    #
    # Scan the files
    #
    f1 = FSC.FSCFile(u"kuku")
    f2 = FSC.FSCSymlink(u"file1")
    file_data = {u"file1": f1, u"file2": f2, u"file3": f1, u"file4": f2}
    self.fsc.reset()
    self.fsc.add_files(file_data)

    # Scan the directory structure
    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    ep = EP.ExclusionProcessor(self.fsc.get_home())
    basedir.scan(ctx, None, ep)
    digest = basedir.get_digest()
    level = basedir.get_level()
    stats = basedir.get_stats()
    
    # Try to restore
    self.fsc.reset()
    restore_dir = Nodes.Directory(backup, None, self.fsc.get_home())
    restore_dir.set_digest(digest)
    restore_dir.set_level(level)
    restore_dir.set_stats(stats)
    restore_dir.restore(ctx)

    self.failUnless(self.fsc.test_files(file_data))
  def test_directory(self):
    # Test that directories are scanned and restored correctly
    backup = Mock.MockBackup(self.fsc.get_home())
    ctx = backup.start_increment("for restoring")

    # Create the directory structure
    file_data = {u"file1" : FSC.FSCFile("kuku"),
        u"dir1": {u"file2" : FSC.FSCFile("kuku"),
      u"file3" : FSC.FSCFile("bebe")}}
    self.fsc.reset()
    self.fsc.add_files(file_data)
    self.failUnless(self.fsc.test_files(file_data))

    # Scan the directory structure
    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    ep = EP.ExclusionProcessor(self.fsc.get_home())
    basedir.scan(ctx, None, ep)
    digest = basedir.get_digest()
    level = basedir.get_level()
    stats = basedir.get_stats()

    # Try to restore
    self.fsc.reset()
    restore_dir = Nodes.Directory(backup, None, self.fsc.get_home())
    restore_dir.set_digest(digest)
    restore_dir.set_level(level)
    restore_dir.set_stats(stats)
    restore_dir.restore(ctx)

    self.failUnless(self.fsc.test_files(file_data))

  def test_directory_unicode(self):
    # Test that unicode directories are scanned and restored correctly
    backup = Mock.MockBackup(self.fsc.get_home())
    ctx = backup.start_increment("for restoring")

    file1 = unicode('\xd1\x85\xd1\x83\xd0\xb9', 'utf8')
    file2 = unicode('\xd0\xb0\xc3\xa4a\xd7\x90', 'utf8')
    file3 = unicode('\xd7\xa9\xd7\x93\xd7\x92\xd7\x9b', 'utf8')
    dir1 = unicode('\xd0\xb0\xd1\x81\xd0\xb4\xd1\x84', 'utf8')
    # Create the directory structure
    file_data = {file1 : FSC.FSCFile("kuku"),
      dir1: {file2 : FSC.FSCFile("kuku"),
      file3 : FSC.FSCFile("bebe")}}
    # print file_data
    self.fsc.reset()
    self.fsc.add_files(file_data)
    self.failUnless(self.fsc.test_files(file_data))

    # Scan the directory structure
    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    ep = EP.ExclusionProcessor(self.fsc.get_home())
    basedir.scan(ctx, None, ep)
    digest = basedir.get_digest()
    level = basedir.get_level()
    stats = basedir.get_stats()

    # Try to restore
    self.fsc.reset()
    restore_dir = Nodes.Directory(backup, None, self.fsc.get_home())
    restore_dir.set_digest(digest)
    restore_dir.set_level(level)
    restore_dir.set_stats(stats)
    restore_dir.restore(ctx)

    self.failUnless(self.fsc.test_files(file_data))
  def test_prev(self):
    # Test that the information from previous versions is taken into account
    backup = Mock.MockBackup(self.fsc.get_home())
    ctx = backup.start_increment("for restoring")
    
    # Create the directory structure
    file_data = {"file1" : FSC.FSCFile("kuku"),
        "dir1": {"file2" : FSC.FSCFile("kuku"),
      "file3" : FSC.FSCFile("bebe")}}
    self.fsc.reset()
    self.fsc.add_files(file_data)

    # Scan the directory structure
    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    ep = EP.ExclusionProcessor(self.fsc.get_home())
    basedir.scan(ctx, None, ep)
    digest = basedir.get_digest()
    level = basedir.get_level()

    time.sleep(1.1)
    file_data["file_new"] = FSC.FSCFile("kukui")
    self.fsc.add_files({'dir1':{"file_new":"kukui"}})
    #self.fsc.add_files({"file_new":"kukui"})
    ctx = backup.start_increment("test prev")
    basedir = Nodes.Directory(backup, None, self.fsc.get_home())
    basedir.scan(ctx, (Nodes.NODE_TYPE_DIR, None, digest, level), ep)
    digest = basedir.get_digest()
    level = basedir.get_level()

    self.assertEquals(ctx.total_nodes, 6)
    # 3 nodes have changed: the new file and the directories that contain it.
    self.assertEquals(ctx.changed_nodes, 3)

suite = unittest.TestLoader().loadTestsFromTestCase(TestNodes)
if __name__ == "__main__":
  unittest.TextTestRunner(verbosity=2).run(suite)

