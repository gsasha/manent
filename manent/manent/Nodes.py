#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os, os.path
import re
import stat
import cStringIO as StringIO
import time
import traceback

import Backup
import Container
import utils.Digest as Digest
import utils.Format as Format
import utils.FileIO as FileIO
import utils.IntegerEncodings as IntegerEncodings
import PackerStream

#----------------------------------------------------
# Node conversion
#----------------------------------------------------
NODE_TYPE_DIR     = 0
NODE_TYPE_FILE    = 1
NODE_TYPE_SYMLINK = 2

STAT_PRESERVED_MODES = [
  stat.ST_MODE,
  stat.ST_UID,
  stat.ST_GID,
  stat.ST_MTIME,
  stat.ST_CTIME,
  #stat.ST_ATIME,
  stat.ST_NLINK,
  stat.ST_INO]

NULL_STAT = {}
for s in STAT_PRESERVED_MODES:
  NULL_STAT[s] = 0
# NULL_STAT = {0:s for s in []}
# NULL_STAT = {a:b for a,b in {}.iteritems()}

#--------------------------------------------------------
# CLASS:Node
#--------------------------------------------------------
class Node:
  """
  Base class of all the filesystem nodes
  """
  def __init__(self, backup, parent, name):
    self.backup = backup
    self.parent = parent
    self.name = name
    self.stats = None
    self.cached_path = None
  def get_digest(self):
    return self.digest
  def set_digest(self, digest):
    self.digest = digest
  def get_level(self):
    return self.level
  def set_level(self, level):
    self.level = level
  #testrunner
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
    node_stat = os.lstat(self.path())
    self.stats = {}
    for mode in STAT_PRESERVED_MODES:
      self.stats[mode] = node_stat[mode]
  def set_stats(self, stats):
    self.stats = stats
  def get_stats(self):
    return self.stats
  
  def serialize_stats(self, base_stats):
    stats = self.get_stats()
    file = StringIO.StringIO()
    if base_stats is not None:
      for mode in STAT_PRESERVED_MODES:
        Format.write_int(file, stats[mode] - base_stats[mode])
    else:
      for mode in STAT_PRESERVED_MODES:
        Format.write_int(file, stats[mode])
    return file.getvalue()
    
  def unserialize_stats(self, file, base_stats):
    stats = {}
    if base_stats is not None:
      for mode in STAT_PRESERVED_MODES:
        val = Format.read_int(file)
        stats[mode] = base_stats[mode] + val
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
  def scan_hlink(self, ctx):
    if self.stats[stat.ST_NLINK] == 1:
      return False
    inode_num = self.stats[stat.ST_INO]
    if ctx.inodes_db.has_key(inode_num):
      self.digest = ctx.inodes_db[inode_num][:Digest.dataDigestSize()]
      level_str = ctx.inodes_db[inode_num][Digest.dataDigestSize():]
      self.level = IntegerEncodings.binary_decode_int_varlen(level_str)
      return True
    return False
  def update_hlink(self, ctx):
    if self.stats[stat.ST_NLINK] == 1:
      return
    inode_num = self.stats[stat.ST_INO]
    if ctx.inodes_db.has_key(inode_num):
      return
    ctx.inodes_db[inode_num] = self.digest +\
      IntegerEncodings.binary_encode_int_varlen(self.level)
  def restore_hlink(self, ctx, dryrun=False):
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
  def scan_prev(self, ctx, prev_num):
    """
    """
    ctx.total_nodes += 1
    if prev_num is None:
      ctx.changed_nodes += 1
      return False

    changed = False

    prev_type, prev_stat, prev_digest, prev_level = prev_num
    if prev_type != self.get_type():
      logging.info("node type differs in the db")
      changed = True
    #elif stat.S_IFMT(self.stats[stat.ST_MODE]) != stat.S_IFMT(prev_stat[stat.ST_MODE]):
      #print "  Node type differs in the fs"
      #changed = True
    elif prev_stat is None:
      logging.info("Base stat not defined")
      changed = True
    elif self.stats[stat.ST_INO] != prev_stat[stat.ST_INO]:
      logging.info("Inode number differs: was %d, now %d" %
        (prev_stat[stat.ST_INO], self.stats[stat.ST_INO]))
      changed = True
    elif self.stats[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
      logging.info("Mtime differs: %d != %d" %
        (self.stats[stat.ST_MTIME], prev_stat[stat.ST_MTIME]))
      changed = True
    elif time.time() - self.stats[stat.ST_MTIME] <= 1.0:
      # The time from the last change is less than the resolution
      # of time() functions
      logging.info("File too recent", prev_stat[stat.ST_MTIME], time.time())
      changed = True
    else:
      #
      # OK, the prev node seems to be the same as this one.
      # Reuse it.
      #
      self.stats = prev_stat
      self.digest = prev_digest
      self.level = prev_level
      return True

    #print "changed node", self.path()
    ctx.changed_nodes += 1
    return False
  def restore_stats(self,
                  restore_chmod=True,
            restore_chown=True,
                  restore_utime=True):
    if restore_chmod:
      os.chmod(self.path(), self.stats[stat.ST_MODE])
    if restore_chown:
      if os.name != 'nt':
        os.lchown(self.path(), self.stats[stat.ST_UID],
                self.stats[stat.ST_GID])
    if restore_utime:
      # Was: first parameter = stat.ST_ATIME, but we canceled it
      # because atime changes all the time and we don't want to
      # back it up.
      os.utime(self.path(), (self.stats[stat.ST_MTIME],
                           self.stats[stat.ST_MTIME]))

#--------------------------------------------------------
# CLASS:File
#--------------------------------------------------------
class File(Node):
  def __init__(self, backup, parent, name):
    Node.__init__(self, backup, parent, name)
  def get_type(self):
    return NODE_TYPE_FILE
  #
  # Scanning and restoring
  #
  def scan(self, ctx, prev_num):
    #
    # Check if we have encountered this file during this scan already
    #
    if self.scan_hlink(ctx):
      logging.info("File %s: HLINK" % self.path())
      return

    #
    # Check if the file is the same as in one of the upper levels
    #
    if self.scan_prev(ctx, prev_num):
      logging.info("File %s: PREV" % self.path())
      return
    
    # --- File not yet in database, process it
    packer = PackerStream.PackerOStream(self.backup, Container.CODE_DATA)
    for data in FileIO.read_blocks(open(self.path(), "rb"),
                          self.backup.get_block_size()):
      packer.write(data)
      
    self.digest = packer.get_digest()
    self.level = packer.get_level()
    self.update_hlink(ctx)

  def test(self, ctx):
    """
    Test that loading the data from the storages is successful
    """
    logging.info("Testing", self.path())
    packer = PackerStream.PackerIStream(self.backup, self.digest,
      self.level)
    for data in FileIO.read_blocks(packer, Digest.dataDigestSize()):
      # Do nothing with the data, just make sure it got loaded
      pass

  def restore(self, ctx):
    """
    Recreate the data from the information stored in the
    backup
    """
    
    logging.info("Restoring " + self.path())
    #
    # Check if the file has already been processed
    # during this pass
    #
    if self.restore_hlink(ctx):
      return

    #
    # No, this file is new. Create it.
    #
    packer = PackerStream.PackerIStream(self.backup, self.digest,
      self.level)
    file = open(self.path(), "wb")
    for data in FileIO.read_blocks(packer, Digest.dataDigestSize()):
      #print "File", self.path(), "reading digest",
      #    base64.b64encode(digest)
      file.write(data)
    file.close()
    
    self.restore_stats()
  
  def request_blocks(self, ctx):
    """
    Put requests for the blocks of the file into the blocks cache
    """
    #
    # Check if the file has already been processed
    # during this pass
    #
    if self.restore_hlink(ctx, dryrun=True):
      return

    logging.info("Requesting blocks for " + self.path())
    #digest_lister = PackerStream.PackerDigestLister(self.backup,
      #self.get_digest())
    #for digest in digest_lister:
      #print "  ", base64.b64encode(digest)
    digest_lister = PackerStream.PackerDigestLister(self.backup,
      self.get_digest(), self.level)
    for digest in digest_lister:
      self.backup.request_block(digest)
  def list_files(self):
    logging.info("F " + base64.b64encode(self.get_digest())[:8] +
        "..." + self.path())

#--------------------------------------------------------
# CLASS:Symlink
#--------------------------------------------------------
class Symlink(Node):
  def __init__(self, backup, parent, name):
    Node.__init__(self, backup, parent, name)
  def get_type(self):
    return NODE_TYPE_SYMLINK
  def scan(self, ctx, prev_num):
    if self.scan_hlink(ctx):
      return

    if self.scan_prev(ctx, prev_num):
      return

    self.link = os.readlink(self.path())

    packer = PackerStream.PackerOStream(self.backup, Container.CODE_DATA)
    packer.write(self.link)

    self.digest = packer.get_digest()
    self.level = packer.get_level()
    self.update_hlink(ctx)
    
  def test(self, ctx):
    logging.info("Testing " + self.path())

    packer = PackerStream.PackerIStream(self.backup, self.digest,
      self.level)
    # Do nothing! We just make sure that it can be loaded.
    self.link = packer.read()

  def restore(self, ctx):
    logging.info("Restoring " + self.path())
    if self.restore_hlink(ctx):
      return

    packer = PackerStream.PackerIStream(self.backup, self.digest,
      self.level)
    self.link = packer.read()
    os.symlink(self.link, self.path())
    # on Linux, there is no use of the mode of a symlink
    # and no way to restore the times of the link itself
    self.restore_stats(restore_chmod=False, restore_utime=False)

  def request_blocks(self, ctx):
    digest_lister = PackerStream.PackerDigestLister(self.backup,
      self.get_digest(), self.level)
    for digest in digest_lister:
      self.backup.request_block(digest)
  def list_files(self):
    logging.info("S " + base64.b64encode(self.get_digest())[:8] +
        '...' + self.path())

#--------------------------------------------------------
# CLASS:Directory
#--------------------------------------------------------
class Directory(Node):
  def __init__(self, backup, parent, name):
    Node.__init__(self, backup, parent, name)
  def get_type(self):
    return NODE_TYPE_DIR
  def scan(self, ctx, prev_num, exclusion_processor):
    """Scan the node, considering data in all the previous increments
    """
    logging.info("Scanning directory", self.path())
    #
    # Process data from previous increments.
    #
    ctx.total_nodes += 1
    # prev data indexed by file, for directory scan
    prev_name_data = {}
    subdirs = []

    #
    # Fetch prev information of this node
    #
    # Find the digest of prev node if it exists
    prev_digest = None
    if prev_num is not None:
      prev_type, prev_stat, prev_digest, prev_level = prev_num
      if prev_type != NODE_TYPE_DIR:
        prev_digest = None
    else:
      # Only dirs stored here, so no need to check node type
      cndb = self.backup.get_completed_nodes_db()
      path_digest = Digest.dataDigest(self.path())
      if cndb.has_key(path_digest):
        prev_data_is = StringIO.StringIO(cndb[path_digest])
        prev_digest = prev_data_is.read(Digest.dataDigestSize())
        prev_level = IntegerEncodings.binary_read_int_varlen(prev_data_is)
        #print "prev_stat_data->", base64.b64encode(prev_data_is.read())
        prev_stat = self.unserialize_stats(prev_data_is, None)
    # Load the data of the prev node
    if prev_digest is not None:
      #print "prev_digest=", prev_digest
      #print "prev_stat= ", prev_stat
      dir_stream = PackerStream.PackerIStream(self.backup, prev_digest,
        prev_level)
      for node_type, node_name, node_stat, node_digest, node_level in\
            self.read_directory_entries(dir_stream, prev_stat):
        if node_type == NODE_TYPE_DIR:
          subdirs.append(node_name)
        prev_name_data[node_name] = ((node_type, node_stat,
                                      node_digest, node_level))

    #
    # Initialize scanning data
    #
    self.children = []
    
    #
    # Scan the directory
    #
    #print "starting scan for", self.path()
    # Scan the files in the directory
    exclusion_processor.filter_files()
    for name in exclusion_processor.get_included_files():
      path = os.path.join(self.path(), name)
      file_mode = os.lstat(path)[stat.ST_MODE]

      if prev_name_data.has_key(name):
        cur_prev = prev_name_data[name]
      else:
        cur_prev = None

      try:
        if stat.S_ISLNK(file_mode):
          node = Symlink(self.backup, self, name)
          node.compute_stats()
          node.scan(ctx, cur_prev)
          self.children.append(node)
        elif stat.S_ISREG(file_mode):
          node = File(self.backup, self, name)
          node.compute_stats()
          node.scan(ctx, cur_prev)
          self.children.append(node)
        else:
          logging.error("Ignoring unrecognized file type " + path)
      except OSError:
        logging.error("OSError accessing " + path)
        traceback.print_exc()
      except IOError, (errno, strerror):
        logging.error("IOError %s accessing '%s' %s" % (errno, strerror, path))
        traceback.print_exc()

    # Scan the subdirs in the directory
    for name in exclusion_processor.get_included_dirs():
      path = os.path.join(self.path(), name)
      file_mode = os.lstat(path)[stat.ST_MODE]

      if prev_name_data.has_key(name):
        cur_prev = prev_name_data[name]
      else:
        cur_prev = None

      try:
        if stat.S_ISDIR(file_mode):
          node = Directory(self.backup, self, name)
          #
          # The order is different here, and it's all because directory can
          # produce temporary digest of its contents during scanning
          #
          node.compute_stats()
          self.children.append(node)
          child_ep = exclusion_processor.descend(name)
          node.scan(ctx, cur_prev, child_ep)
        else:
          logging.error("Ignoring unrecognized file type " + path)
      except OSError:
        logging.error("OSError accessing " + path)
        traceback.print_exc()
      except IOError, (errno, strerror):
        logging.error("IOError %s accessing '%s'" % (strerror, path))
        traceback.print_exc()

    self.write(ctx)
    self.children = None

    #
    # Update the current dir in completed_nodes_db
    #
    cndb = self.backup.get_completed_nodes_db()
    for subdir in subdirs:
      subdir_path = os.path.join(self.path(), subdir)
      subdir_path_digest = Digest.dataDigest(subdir_path)
      if cndb.has_key(subdir_path_digest):
        del cndb[subdir_path_digest]
    if self.stats is not None:
      # Stats are empty for the root node, but we don't want to store
      # it in the cndb, because at this point we're already done with the
      # increment anyway
      cndb[Digest.dataDigest(self.path())] =\
        self.digest + IntegerEncodings.binary_encode_int_varlen(self.level) +\
        self.serialize_stats(None)

    if self.digest != prev_digest:
      #print "changed node", self.path()
      ctx.changed_nodes += 1

  def write(self, ctx):
    """
    Write the info of the current dir to database
    """
    packer = PackerStream.PackerOStream(self.backup, Container.CODE_DIR)
    # sorting is an optimization to make everybody access files in the same order,
    # TODO: measure if this really makes things faster (probably will with a btree db)
    for child in self.children:
      Format.write_int(packer, child.get_type())
      Format.write_string(packer, child.get_name())
      packer.write(child.get_digest())
      packer.write(IntegerEncodings.binary_encode_int_varlen(child.get_level()))
      stats_str = child.serialize_stats(self.get_stats())
      packer.write(stats_str)
    
    self.digest = packer.get_digest()
    self.level = packer.get_level()
    
  def test(self, ctx):
    logging.info("Testing", self.path())

    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer, self.stats):
      if node_type == NODE_TYPE_DIR:
        node = Directory(self.backup, self, node_name)
      elif node_type == NODE_TYPE_FILE:
        node = File(self.backup, self, node_name)
      elif node_type == NODE_TYPE_SYMLINK:
        node = Symlink(self.backup, self, node_name)
      else:
        raise Exception("Unknown node type [%s]"%node_type)
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      node.test(ctx)
  
  def restore(self, ctx):
    logging.info("Restoring", self.path())
    if self.parent != None:
      os.mkdir(self.path())

    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer, self.stats):
      if node_type == NODE_TYPE_DIR:
        node = Directory(self.backup, self, node_name)
      elif node_type == NODE_TYPE_FILE:
        node = File(self.backup, self, node_name)
      elif node_type == NODE_TYPE_SYMLINK:
        node = Symlink(self.backup, self, node_name)
      else:
        raise Exception("Unknown node type [%s]"%node_type)
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      node.restore(ctx)
    if self.stats is not None:
      # Root node has no stats
      self.restore_stats()

  def request_blocks(self, ctx):
    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer, self.stats):
      if node_type == NODE_TYPE_DIR:
        node = Directory(self.backup, self, node_name)
      elif node_type == NODE_TYPE_FILE:
        node = File(self.backup, self, node_name)
      elif node_type == NODE_TYPE_SYMLINK:
        node = Symlink(self.backup, self, node_name)
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      node.request_blocks(ctx)

  def list_files(self):
    print "D", base64.b64encode(self.digest)[:8]+'...', self.path()
    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer, self.stats):
      if node_type == NODE_TYPE_DIR:
        node = Directory(self.backup, self, node_name)
      elif node_type == NODE_TYPE_FILE:
        node = File(self.backup, self, node_name)
      elif node_type == NODE_TYPE_SYMLINK:
        node = Symlink(self.backup, self, node_name)
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      node.list_files()
  
  def read_directory_entries(self, file, base_stats):
    while True:
      node_type = Format.read_int(file)
      if node_type is None:
        raise StopIteration
      node_name = Format.read_string(file)
      node_digest = file.read(Digest.dataDigestSize())
      node_level = IntegerEncodings.binary_read_int_varlen(file)
      node_stat = self.unserialize_stats(file, base_stats)
      yield (node_type, node_name, node_stat, node_digest, node_level)
