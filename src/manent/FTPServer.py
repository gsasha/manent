#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

from pyftpdlib import ftpserver
import StringIO
import base64
import logging
import os
import stat
import sys
import tarfile
import time
import traceback

# On Windows, these modules are not supported.
try:
  import pwd
  import grp
except ImportError:
  pwd = grp = None

import Backup
import Nodes

class Node:
  def __init__(self, backup):
    self.backup = backup
  def is_file(self):
    return False
  def is_directory(self):
    return False
  def seek(self, pos, whence=0):
    raise OSError(1, "not supported")
  def write(self, data):
    raise OSError(1, "operation not permitted")

class FileNode(Node):
  def __init__(self, backup, name, node):
    Node.__init__(self, backup)
    self.name = name
    self.node = node
    self.closed = False
    self.contents = None
  def is_file(self):
    return True
  def get_stats(self):
    stats = self.node.get_stats()
    return stats
  def close(self):
    pass
  def read(self, size=None):
    if self.contents is None:
      self.contents = StringIO.StringIO()
      self.node.request_blocks(None)
      self.node.retrieve(self.contents)
      self.contents.seek(0)

    return self.contents.read(size)

class SymlinkNode(Node):
  def __init__(self, backup):
    Node.__init__(self, backup)
    self.closed = False
    self.contents = StringIO.StringIO("symlink")
  def get_stats(self):
    stats = self.node.get_stats()
    return stats
  def close(self):
    pass
  def read(self, size=None):
    return self.contents.read(size)

def make_root(backup, name, fs_digest, fs_level, fs_stats):
  n = DirectoryNode(backup)
  n.node = Nodes.Directory(backup, None, u"$$$/")
  n.node.set_stats(Nodes.NULL_STAT)
  n.node.set_digest(fs_digest)
  n.node.set_level(fs_level)
  n.node.set_stats(fs_stats)
  n.node.read_child_nodes()
  return n

class DirectoryNode(Node):
  def __init__(self, backup):
    Node.__init__(self, backup)
  def is_directory(self):
    return True
  def get_stats(self):
    stats = self.node.get_stats()
    return stats
  def get_child_names(self):
    return self.node.get_child_node_names()
  def get_child_node(self, name):
    print "Node %s getting child %s" % (self.node.path(), name)
    node_type, node_stat, node_digest, node_level =\
        self.node.get_child_node_data(name)
    if node_type == Nodes.NODE_TYPE_DIR:
      node = Nodes.Directory(self.backup, self.node, unicode(name, 'utf8'))
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      n = DirectoryNode(self.backup)
      n.node = node
      n.node.read_child_nodes()
      return n
    elif node_type == Nodes.NODE_TYPE_FILE:
      node = Nodes.File(self.backup, self.node, unicode(name, 'utf8'))
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      n = FileNode(self.backup, name, node)
      return n
    elif node_type == Nodes.NODE_TYPE_SYMLINK:
      node = Nodes.Symlink(self.backup, self.node, unicode(name, 'utf8'))
      node.set_stats(node_stat)
      node.set_digest(node_digest)
      node.set_level(node_level)
      n = SymlinkNode(self.backup)
      n.node = node
      return n

class IncrementDescriptorFile(Node):
  def __init__(self, backup, increment, name):
    Node.__init__(self, backup)
    self.closed = False
    self.increment = increment
    self.name = name
    self.contents = StringIO.StringIO()
    self.contents.write("Manent backup increment\n")
    self.contents.write("started: %s\n" %
        time.ctime(float(self.increment.get_attribute("ctime"))))
    self.contents.write("finished: %s\n" %
        time.ctime(float(self.increment.get_attribute("ftime"))))
    self.contents.write("fs: %s:%s\n" %
        (base64.b64encode(self.increment.get_attribute("fs_digest")),
          self.increment.get_attribute("fs_level")))
    self.contents.write("hostname: %s\n" %
        self.increment.get_attribute("hostname"))
    self.contents.write("backup: %s\n" %
        self.increment.get_attribute("backup_label"))
    self.contents.write("comment: %s\n" %
        self.increment.get_attribute("comment"))
    self.size = self.contents.tell()
    self.contents.seek(0)
  def is_file(self):
    return True
  def get_stats(self):
    stats = {
        stat.ST_MODE: 0444,
        stat.ST_NLINK: 1,
        stat.ST_GID: 0,
        stat.ST_UID: 0,
        stat.ST_SIZE: self.size,
        stat.ST_MTIME: 0,
        }
    return stats
  def close(self):
    pass
  def read(self, size=None):
    return self.contents.read(size)

class IncrementNode(Node):
  def __init__(self, backup):
    Node.__init__(self, backup)
    self.backup = backup
    storage_manager = self.backup.storage_manager
    active_storage_idx = storage_manager.get_active_storage_index()
    increment_manager = self.backup.increment_manager
    increments = increment_manager.get_increments()
    self.increments = {}
    self.increment_descriptors = {}
    for idx in increments[active_storage_idx]:
      increment = increment_manager.get_increment(active_storage_idx, idx)
      self.increments["%s.%d" %
          (increment.get_attribute("backup_label"), idx)] = increment
      self.increment_descriptors["%s.%d.txt" %
          (increment.get_attribute("backup_label"), idx)] = increment
  def is_directory(self):
    return True
  def get_stats(self):
    stats = {
        stat.ST_MODE: 040000,
        stat.ST_NLINK: 0,
        stat.ST_GID: 0,
        stat.ST_UID: 0,
        stat.ST_SIZE: 0,
        stat.ST_MTIME: 0,
        }
    return stats
  def get_child_node(self, name):
    logging.info("IncrementNode getting child %s" % name)
    if self.increment_descriptors.has_key(name):
      return IncrementDescriptorFile(self.backup,
          self.increment_descriptors[name], name)
    elif self.increments.has_key(name):
      increment = self.increments[name]
      return make_root(self.backup, name,
          increment.get_fs_digest(), increment.get_fs_level(),
          increment.get_fs_stats())
    else:
      raise OSError(1, "File not found: %s" % name)
  def get_child_names(self):
    for name in self.increments.iterkeys():
      yield name
    for name in self.increment_descriptors.iterkeys():
      yield name
  def is_directory(self):
    return True

class ManentFilesystemGenerator:
  def __init__(self, backup):
    self.backup = backup
  def __call__(self):
    return ManentFilesystem(self.backup)

class ManentFilesystem(ftpserver.AbstractedFS):
  def __init__(self, backup):
    self.backup = backup
    self.cwd = "/"
    self.root_node = IncrementNode(self.backup)
  def open(self, path, mode):
    path = self.fs2ftp(path)
    print "********** open %s" % path
    # All paths begin with a "/"
    node = self.__get_node(path)
    if not node.is_file():
      raise OSError("%s is not a file" % path)
    return node
  def chdir(self, path):
    path = self.fs2ftp(path)
    print "doing chdir to ", path
    self.cwd = path
  def mkdir(self, path):
    print "********** mkdir", path
    raise OSError(1, 'Operation not permitted')
  def listdir(self, path):
    print "********** listdir", path
  def rmdir(self, path):
    print "********** rmdir", path
    raise OSError(1, 'Operation not permitted')
  def remove(self, path):
    print "********** remove", path
    raise OSError(1, 'Operation not permitted')
  def rename(self, src, dst):
    print "********** rename", src, dst
    raise OSError(1, 'Operation not permitted')
  def isfile(self, path):
    print "********** isfile", path
    return True
  def isdir(self, path):
    print "********** isdir", path
    return False
  def getsize(self, path):
    print "********** getsize", path
    return 17
  def getmtime(self, path):
    print "********** getmtime", path
    return 17
  def realpath(self, path):
    print "********** realpath", path
    return path
  def lexists(self, path):
    print "********** lexists", path
    return False
  def validpath(self, path):
    # We can't check validpath since fs2ftp uses this method...
    print "********** validpath", path
    return True
  def get_list_dir(self, path):
    path = self.fs2ftp(path)
    print "******** listing files in %s" % path
    node = self.__get_node(path)
    for name in node.get_child_names():
      stats = node.get_child_node(name).get_stats()
      perms = tarfile.filemode(stats[stat.ST_MODE])
      nlink = stats[stat.ST_NLINK]
      if pwd and grp:
        print "QUERYING PWD"
        try:
          user_name = pwd.getpwuid(stats[stat.ST_UID]).pw_name
          group_name = grp.getgrgid(stats[stat.ST_GID]).gr_name
        except:
          traceback.print_exc()
          user_name = str(stats[stat.ST_UID])
          group_name = str(stats[stat.ST_GID])
      else:
        user_name = str(stats[stat.ST_UID])
        group_name = str(stats[stat.ST_GID])
      time_str = time.strftime("%b %d %H:%M",
          time.localtime(stats[stat.ST_MTIME]))

      yield "%s %3d %-8s %-8s %8d %s %s\r\n" % (
          perms,
          nlink,
          user_name,
          group_name,
          stats[stat.ST_SIZE],
          time_str,
          name)
  def format_list(self, basedir, listing, ignore_err=True):
    print "********** format_list %s, %s" % (str(basedir), str(listing))
  def get_stat_dir(self, rawline):
    print "********** get_stat_dir", path
    raise OSError(40, 'unsupported')
  def format_mslx(self, basedir, listing, perms, facts, ignore_err=True):
    print "********** format_mslx", basedir
    raise OSError(40, 'unsupproted')
  def __get_node(self, path):
    while path.endswith("/"):
      path = path[:-1]
    elements = path.split("/")
    if elements[0] == "":
      elements = elements[1:]
    cur_node = self.root_node
    print  "Starting with cur_node=%s" % str(cur_node)
    for element in elements:
      cur_node = cur_node.get_child_node(element)
      print "Getting child %s -> %s" % (element, cur_node)
    return cur_node

def serve(backup, port):
    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = ftpserver.DummyAuthorizer()

    # Define a new user having full r/w permissions and a read-only
    # anonymous user
    authorizer.add_user('user', '', os.getcwd(), perm='elradfmw')
    authorizer.add_anonymous(os.getcwd())

    # Instantiate FTP handler class
    ftp_handler = ftpserver.FTPHandler
    ftp_handler.authorizer = authorizer

    # Define a customized banner (string returned when client connects)
    ftp_handler.banner = "pyftpdlib %s based ftpd ready." %ftpserver.__ver__

    ftp_handler.abstracted_fs = ManentFilesystemGenerator(backup)

    # Specify a masquerade address and the range of ports to use for
    # passive connections.  Decomment in case you're behind a NAT.
    #ftp_handler.masquerade_address = '151.25.42.11'
    #ftp_handler.passive_ports = range(60000, 65535)

    # Instantiate FTP server class and listen to 0.0.0.0:21
    address = ('', port)
    ftpd = ftpserver.FTPServer(address, ftp_handler)

    # set a limit for connections
    ftpd.max_cons = 256
    ftpd.max_cons_per_ip = 5

    # start ftp server
    ftpd.serve_forever()
