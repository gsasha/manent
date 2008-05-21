#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import cStringIO as StringIO
import socket
import time

import Container
import utils.Digest as Digest
import utils.IntegerEncodings as IE

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
  def __init__(self, block_database, db):
    self.block_database = block_database
    self.db = db

    self.readonly = None

  def get_index(self):
    return int(self.attributes["index"])
  def get_fs_digest(self):
    return self.attributes["fs_digest"]
  def get_fs_level(self):
    return int(self.attributes["fs_level"])
  def get_attribute(self, key):
    return self.attributes[key]

  #
  # Methods for manipulating a newly created increment
  #
  def start(self, storage_index, index, backup_label, comment):
    if self.readonly is not None:
      raise Exception("Attempt to start an existing increment")
    self.readonly = False
    
    self.attributes = {
        "index": str(index),
        "storage_index": str(storage_index),
        "backup_label": backup_label,
        "comment": comment,
        "ctime": str(time.time()),
        "hostname": socket.gethostname()[0],
        "fs_digest": None,
        "fs_level": None,
    }

  def finalize(self, fs_digest, fs_level):
    if self.readonly != False:
      raise Exception("Increment already finalized")
    
    self.attributes["fs_digest"] = fs_digest
    self.attributes["fs_level"] = str(fs_level)
    self.attributes["ftime"] = str(time.time())
    self.readonly = True
    
    #print "Finalizing increment", self.fs_digest
    PREFIX = "Increment.%s.%s." % (self.attributes["storage_index"],
        self.attributes["index"])
    for key, val in self.attributes.iteritems():
      self.db[PREFIX + key] = val
    message = self.__compute_message()
    digest = Digest.dataDigest(message)
    self.block_database.add_block(
        digest, Container.CODE_INCREMENT_DESCRIPTOR, message)
    return digest

  #
  # Loading an existing increment from db
  #
  def load(self, storage_index, index):
    if self.readonly != None:
      raise "Attempt to load an existing increment"

    PREFIX = "Increment.%s.%s." % (str(storage_index), str(index))
    self.attributes = {}
    for key, val in self.db.iteritems_prefix(PREFIX):
      self.attributes[key[len(PREFIX):]] = val
    
    assert self.attributes["index"] == str(index)

    self.readonly = True

  #
  # Restoring an increment from backup to db
  #
  def reconstruct(self, digest):
    if self.readonly != None:
      raise "Attempt to restore an existing increment"

    #
    # Parse the message from the storage
    #
    storage_index = self.block_database.get_storage_index(digest)
    message = self.block_database.load_block(digest)
    self.attributes = self.__parse_message(message)
    assert self.attributes["storage_index"] == str(storage_index)

    #
    # Update the data in the db
    #
    PREFIX = "Increment.%s.%s." % (self.attributes["storage_index"],
        self.attributes["index"])
    for key, val in self.attributes.iteritems():
      self.db[PREFIX + key] = val

    self.readonly = True
  def __compute_message(self):
    m = StringIO.StringIO()
    for key, val in self.attributes.iteritems():
      m.write("%s=%s\n" % (key, base64.b64encode(val)))
    return m.getvalue()

  def __parse_message(self, message):
    items = {}
    stream = StringIO.StringIO(message)
    for line in stream:
      key, value = line.strip().split("=", 1)
      items[key] = base64.b64decode(value)
    return items
