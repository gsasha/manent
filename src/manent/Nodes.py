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
import PackerStream
import utils.Digest as Digest
import utils.FileIO as FileIO
import utils.Format as Format
import utils.IntegerEncodings as IntegerEncodings

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
  stat.ST_SIZE,
  stat.ST_INO]

NULL_STAT = {}
for s in STAT_PRESERVED_MODES:
  NULL_STAT[s] = 0
# NULL_STAT = {0:s for s in []}
# NULL_STAT = {a:b for a,b in {}.iteritems()}

def serialize_stats(stats):
  file = StringIO.StringIO()
  for mode in STAT_PRESERVED_MODES:
    Format.write_int(file, stats[mode])
  return file.getvalue()
    
def unserialize_stats(file):
  stats = {}
  for mode in STAT_PRESERVED_MODES:
    stats[mode] = Format.read_int(file)
  return stats

#--------------------------------------------------------
# CLASS:Node
#--------------------------------------------------------
class Node:
  """
  Base class of all the filesystem nodes
  """
  def __init__(self, backup, parent, name):
    self.backup = backup
    # print "------------", name
    self.parent = parent
    assert type(name) == type(u'')
    self.name = name
    self.stats = None
    self.cached_path = None

    self.weight = 0.0
    self.processed_percent = 0.0
    self.cur_scanned_child = None
  def get_digest(self):
    return self.digest
  def set_digest(self, digest):
    self.digest = digest
  def get_level(self):
    return self.level
  def set_level(self, level):
    self.level = level
  def set_weight(self, weight):
    self.weight = weight
  def get_percent_done(self):
    raise Exception("Abstract method")
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
  
  #-----------------------------------------------------
  # Support for scanning hard links:
  # 
  # see if the current file is a hard link to another file
  # that has already been scanned. If so, reuse it.
  #
  def scan_hlink(self, ctx):
    if os.name == 'nt':
      # Inode numbers not reported, so we canot detect hard links.
      return False
    if self.stats[stat.ST_NLINK] == 1:
      logging.debug("File %s has NLINK=1, can't be hard link", self.path())
      return False
    inode_num = self.stats[stat.ST_INO]
    if ctx.inodes_db.has_key(inode_num):
      self.digest = ctx.inodes_db[inode_num][:Digest.dataDigestSize()]
      level_str = ctx.inodes_db[inode_num][Digest.dataDigestSize():]
      self.level = IntegerEncodings.binary_decode_int_varlen(level_str)
      return True
    return False
  def update_hlink(self, ctx):
    if os.name == 'nt':
      return
    if self.stats[stat.ST_NLINK] == 1:
      return
    inode_num = self.stats[stat.ST_INO]
    if ctx.inodes_db.has_key(inode_num):
      return
    ctx.inodes_db[inode_num] = self.digest +\
      IntegerEncodings.binary_encode_int_varlen(self.level)
  def restore_hlink(self, ctx, dryrun=False):
    if os.name == 'nt':
      return False
    if self.stats[stat.ST_NLINK] == 1:
      logging.debug("According to NLINK, file %s is not HLINK", self.path())
      return False
    if not ctx.inodes_db.has_key(self.digest):
      ctx.inodes_db[self.digest] = self.path()
      logging.debug("Hlink not found in inodes_db")
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
      cndb = self.backup.get_completed_nodes_db()
      path_digest = Digest.dataDigest(self.path().encode('utf8'))
      if cndb.has_key(path_digest):
        prev_data_is = StringIO.StringIO(cndb[path_digest])
        prev_digest = prev_data_is.read(Digest.dataDigestSize())
        prev_level = IntegerEncodings.binary_read_int_varlen(prev_data_is)
        prev_type = IntegerEncodings.binary_read_int_varlen(prev_data_is)
        #print "prev_stat_data->", base64.b64encode(prev_data_is.read())
        prev_stat = unserialize_stats(prev_data_is)
      else:
        ctx.changed_nodes += 1
        return False
    else:
      prev_type, prev_stat, prev_digest, prev_level = prev_num

    changed = False

    if prev_type != self.get_type():
      logging.info("node type differs in the db")
      changed = True
    #elif (stat.S_IFMT(self.stats[stat.ST_MODE]) !=
           #stat.S_IFMT(prev_stat[stat.ST_MODE])):
      #print "  Node type differs in the fs"
      #changed = True
    elif prev_stat is None:
      logging.info("Base stat not defined")
      changed = True
    elif self.stats[stat.ST_INO] != prev_stat[stat.ST_INO]:
      logging.info("Inode of %s differs: was %d, now %d" %
        (self.path(), prev_stat[stat.ST_INO], self.stats[stat.ST_INO]))
      changed = True
    elif self.stats[stat.ST_MTIME] != prev_stat[stat.ST_MTIME]:
      logging.info("Mtime of %s differs: %d != %d" %
        (self.path(), self.stats[stat.ST_MTIME], prev_stat[stat.ST_MTIME]))
      changed = True
    elif time.time() - self.stats[stat.ST_MTIME] <= 1.0:
      # The time from the last change is less than the resolution
      # of time() functions
      logging.info("File %s too recent %d : %d" %
          (self.path(), prev_stat[stat.ST_MTIME], time.time()))
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
    if restore_chown:
      if os.name != 'nt':
        os.lchown(self.path(), self.stats[stat.ST_UID],
                self.stats[stat.ST_GID])
    if restore_utime:
      # Was: first parameter = stat.ST_ATIME, but we canceled it
      # because atime changes all the time and we don't want to
      # back it up.
      if os.name == 'nt' and self.get_type() == NODE_TYPE_DIR:
        # Windows can't set utime on directories.
        pass
      else:
        os.utime(self.path(), (self.stats[stat.ST_MTIME],
                               self.stats[stat.ST_MTIME]))
    if restore_chmod:
      os.chmod(self.path(), self.stats[stat.ST_MODE])

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
    self.compute_stats()
    #
    # Check if we have encountered this file during this scan already
    #
    ctx.num_visited_files_reporter.increment(1)
    ctx.current_scanned_file_reporter.set(self.path())

    if self.scan_hlink(ctx):
      logging.info("File %s: HLINK" % self.path())
      return

    #
    # Check if the file is the same as in one of the upper levels
    #
    if self.scan_prev(ctx, prev_num):
      logging.debug("File %s: PREV" % self.path())
      ctx.num_prev_files_reporter.increment(1)
      return
    
    # --- File not yet in database, process it
    file_size = 0
    packer = PackerStream.PackerOStream(self.backup, Container.CODE_DATA)
    handle = open(self.path(), "rb")
    for data in FileIO.read_blocks(handle, self.backup.get_block_size()):
      packer.write(data)
      file_size += len(data)
      ctx.num_total_blocks_reporter.increment(1)
      ctx.size_total_blocks_reporter.increment(len(data))
      ctx.update_scan_status()
    handle.close()
      
    self.digest = packer.get_digest()
    self.level = packer.get_level()
    self.update_hlink(ctx)

    logging.info("Scanned file %s size:%d new_blocks:%d new_blocks_size:%d" %
        (self.path(), file_size, packer.get_num_new_blocks(),
          packer.get_size_new_blocks()))

    ctx.num_scanned_files_reporter.increment(1)
    if packer.get_num_new_blocks() != 0:
      ctx.num_new_blocks_reporter.increment(packer.get_num_new_blocks())
      ctx.size_new_blocks_reporter.increment(packer.get_size_new_blocks())
      ctx.num_changed_files_reporter.increment(1)
      ctx.changed_files_reporter.append(self.path())

    if file_size > 256 * 1024:
      logging.debug("File %s is big enough to register in cndb" %
          self.path())
      cndb = self.backup.get_completed_nodes_db()
      assert self.stats is not None
      path_digest = Digest.dataDigest(self.path().encode('utf8'))
      encoded = (self.digest +
          IntegerEncodings.binary_encode_int_varlen(self.level) +
          IntegerEncodings.binary_encode_int_varlen(self.get_type()) +
          serialize_stats(self.get_stats()))

      if not cndb.has_key(path_digest) or cndb[path_digest] != encoded:
        cndb[path_digest] = encoded

  def test(self, ctx):
    """
    Test that loading the data from the storages is successful
    """
    logging.info("Testing " + self.path())
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
  
  def retrieve(self, stream):
    """
    Recreate the data from the information stored in the backup into the given
    stream
    """
    logging.info("Retrieving file " + self.path())
    packer = PackerStream.PackerIStream(self.backup, self.digest,
        self.level)
    for data in FileIO.read_blocks(packer, Digest.dataDigestSize()):
      stream.write(data)

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
    self.compute_stats()
    ctx.num_visited_symlinks_reporter.increment(1)

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
    
    logging.info("Scanned symlink %s size:%d new_blocks:%d new_blocks_size:%d" %
        (self.path(), len(self.link), packer.get_num_new_blocks(),
          packer.get_size_new_blocks()))

    ctx.num_scanned_symlinks_reporter.increment(1)
    if packer.get_num_new_blocks() != 0:
      ctx.num_new_blocks_reporter.increment(packer.get_num_new_blocks())
      ctx.size_new_blocks_reporter.increment(packer.get_size_new_blocks())
      ctx.num_changed_symlinks_reporter.increment(1)
      ctx.changed_symlinks_reporter.append(self.path())
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
    try:
      os.symlink(self.link, self.path())
      # on Linux, there is no use of the mode of a symlink
      # and no way to restore the times of the link itself
      self.restore_stats(restore_chmod=False, restore_utime=False)
    except:
      logging.info("Failed restoring symlink ", self.path(), " to ", self.link)


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
    logging.debug("Scanning directory " + self.path())
    self.compute_stats()
    ctx.num_visited_dirs_reporter.increment(1)
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
      cndb = self.backup.get_completed_nodes_db()
      path_digest = Digest.dataDigest(self.path().encode('utf8'))
      if cndb.has_key(path_digest):
        prev_data_is = StringIO.StringIO(cndb[path_digest])
        prev_digest = prev_data_is.read(Digest.dataDigestSize())
        prev_level = IntegerEncodings.binary_read_int_varlen(prev_data_is)
        prev_type = IntegerEncodings.binary_read_int_varlen(prev_data_is)
        #print "prev_stat_data->", base64.b64encode(prev_data_is.read())
        prev_stat = unserialize_stats(prev_data_is)
        if prev_type != self.get_type():
          logging.debug("Node from cndb is not a directory!")
          prev_digest = None
    # Load the data of the prev node
    if prev_digest is not None:
      dir_stream = PackerStream.PackerIStream(self.backup, prev_digest,
        prev_level)
      for node_type, node_name, node_stat, node_digest, node_level in\
            self.read_directory_entries(dir_stream):
        if node_type == NODE_TYPE_DIR:
          subdirs.append(node_name)
        prev_name_data[node_name] = ((node_type, node_stat,
                                      node_digest, node_level))

    #
    # Scan the directory
    #
    exclusion_processor.filter_files()

    # Initialize scanning data
    self.children = []
    num_children = len(exclusion_processor.get_included_files() +
        exclusion_processor.get_included_dirs())
    processed_children = 0.0

    # Scan the files in the directory
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
          node.scan(ctx, cur_prev)
          self.children.append(node)
        elif stat.S_ISREG(file_mode):
          node = File(self.backup, self, name)
          node.scan(ctx, cur_prev)
          self.children.append(node)
        else:
          ctx.unrecognized_files_reporter.append(path)
          logging.info("Ignoring unrecognized file type " + path)
      except OSError:
        logging.info("OSError accessing " + path)
        ctx.oserror_files_reporter.append(path)
        # traceback.print_exc()
      except IOError, (errno, strerror):
        logging.info("IOError %s accessing '%s' %s" % (errno, strerror, path))
        ctx.ioerror_files_reporter.append(path)
        # traceback.print_exc()
      finally:
        processed_children += 1
        self.processed_percent = processed_children / num_children
        ctx.update_scan_status()

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
          node.set_weight(self.weight / num_children)
          self.cur_scanned_child = node
          #
          # The order is different here, and it's all because directory can
          # produce temporary digest of its contents during scanning
          #
          child_ep = exclusion_processor.descend(name)
          node.scan(ctx, cur_prev, child_ep)
          # TODO(gsasha): this is a temporary hack. This statement should be two
          # lines above, as the comment says.
          self.children.append(node)
        else:
          ctx.unrecognized_files_reporter.append(path)
          logging.info("Ignoring unrecognized file type " + path)
      except OSError:
        logging.info("OSError accessing " + path)
        ctx.oserror_files_reporter.append(path)
        # traceback.print_exc()
      except IOError, (errno, strerror):
        logging.info("IOError %s accessing '%s'" % (strerror, path))
        ctx.ioerror_files_reporter.append(path)
        # traceback.print_exc()
      finally:
        processed_children += 1
        self.processed_percent = processed_children / num_children
        ctx.update_scan_status()
        self.cur_scanned_child = None

    num_new_blocks, size_new_blocks = self.write(ctx)
    if num_new_blocks != 0:
      logging.info("Scanned dir %s size:%d new_blocks:%d new_blocks_size:%d" %
        (self.path(), len(self.children), num_new_blocks,
          size_new_blocks))

    ctx.num_scanned_dirs_reporter.increment(1)
    if num_new_blocks != 0:
      ctx.num_new_blocks_reporter.increment(num_new_blocks)
      ctx.size_new_blocks_reporter.increment(size_new_blocks)
      ctx.num_changed_dirs_reporter.increment(1)
      ctx.changed_dirs_reporter.append(self.path())

    self.children = None
    #
    # Update the current dir in completed_nodes_db
    #
    cndb = self.backup.get_completed_nodes_db()
    for subdir in subdirs:
      subdir_path = os.path.join(self.path(), subdir)
      subdir_path_digest = Digest.dataDigest(subdir_path.encode('utf8'))
      if cndb.has_key(subdir_path_digest):
        del cndb[subdir_path_digest]
    if self.stats is not None:
      # Stats are empty for the root node, but we don't want to store
      # it in the cndb, because at this point we're already done with the
      # increment anyway
      digest = Digest.dataDigest(self.path().encode('utf8'))
      encoded = (self.digest +
          IntegerEncodings.binary_encode_int_varlen(self.level) +
          IntegerEncodings.binary_encode_int_varlen(self.get_type()) +
          serialize_stats(self.get_stats()))

      if not cndb.has_key(digest) or cndb[digest] != encoded:
        cndb[digest] = encoded
        
    if self.digest != prev_digest:
      #print "changed node", self.path()
      ctx.changed_nodes += 1

  def get_percent_done(self):
    if self.cur_scanned_child is None:
      return self.weight * self.processed_percent
    else:
      return (self.weight * self.processed_percent +
          self.cur_scanned_child.get_percent_done())

  def write(self, ctx):
    """
    Write the info of the current dir to database
    """
    packer = PackerStream.PackerOStream(self.backup, Container.CODE_DIR)
    # sorting is an optimization to make everybody access files in the same
    # order.
    # TODO: measure if this really makes things faster
    # (probably will with a btree db)
    for child in self.children:
      Format.write_int(packer, child.get_type())
      Format.write_string(packer, child.get_name().encode('utf8'))
      packer.write(child.get_digest())
      packer.write(IntegerEncodings.binary_encode_int_varlen(child.get_level()))
      stats_str = serialize_stats(child.get_stats())
      packer.write(stats_str)
    
    self.digest = packer.get_digest()
    self.level = packer.get_level()
    return (packer.get_num_new_blocks(), packer.get_size_new_blocks())
    
  def test(self, ctx):
    logging.info("Testing " + self.path())

    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer):
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
    logging.info("Restoring " + self.path())
    if self.parent != None:
      try:
        os.mkdir(self.path())
      except:
        logging.error("Failed creating directory " + self.path())
        return

    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer):
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
    try:
      self.restore_stats()
    except:
      logging.error("Failed restoring stats for " + self.path())
      traceback.print_exc()
      return

  def list_files(self):
    print "D", base64.b64encode(self.digest)[:8]+'...', self.path()
    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
      self.read_directory_entries(packer):
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
  
  def read_child_nodes(self):
    self.children_nodes_data = {}
    self.children_nodes = {}
    packer = PackerStream.PackerIStream(self.backup, self.get_digest(),
      self.get_level())
    for (node_type, node_name, node_stat, node_digest, node_level) in\
        self.read_directory_entries(packer):
      self.children_nodes_data[node_name] = (node_type, node_stat,
                                   node_digest, node_level)
  def get_child_node(self, name):
    if not self.children_nodes_data.has_key(name):
      return None
    if not self.children_nodes.has_key(name):
      node_type, node_stat, node_digest, node_level =\
          self.children_nodes_data[name]
      if node_type == NODE_TYPE_DIR:
        node = Directory(self.backup, self, node_name)
      elif node_type == NODE_TYPE_FILE:
        node = File(self.backup, self, node_name)
      elif node_type == NODE_TYPE_SYMLINK:
        node = Symlink(self.backup, self, node_name)
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      self.children_nodes[name] = node
    return self.children_nodes[name]
  def get_child_node_data(self, name):
    return self.children_nodes_data[name]
  def get_child_node_names(self):
    return self.children_nodes_data.keys()

  def read_directory_entries(self, file):
    while True:
      node_type = Format.read_int(file)
      if node_type is None:
        raise StopIteration
      node_name = Format.read_string(file)
      node_digest = file.read(Digest.dataDigestSize())
      node_level = IntegerEncodings.binary_read_int_varlen(file)
      node_stat = unserialize_stats(file)
      try:
        node_name_decoded = unicode(node_name, 'utf8')
        yield (node_type, node_name_decoded, node_stat, node_digest, node_level)
      except:
        logging.info("Encountered bad file name in " + self.path())
