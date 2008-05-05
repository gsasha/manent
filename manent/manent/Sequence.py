#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import re
import shutil
import cStringIO as StringIO
import tempfile
import traceback

import Config
import Container
import utils.IntegerEncodings as IE
import Storage

PIGGYBACK_MASK = sum([(1<<x) for x in range(2, 32, 2)])
MAX_PIGGYBACK_HEADERS = 16 << 10
def _last_set_bit(num):
  return num & ~(num - 1)

class Sequence:
  """Represents and manages a single sequence of containers.
     Containers are numbered sequentially from 0.
     Also controls the piggy-backing of containers."""
  def __init__(self, storage, seq_id, is_active):
    self.storage = storage
    self.sequence_id = seq_id
    if self._get_config_db().has_key(self._key("next_index")):
      self.next_container_idx = int(
        self._get_config_db()[self._key("next_index")])
    else:
      self.next_container_idx = 0
      self._get_config_db()[self._key("next_index")] = (
        self.next_container_idx)
    self.is_active = None
    self.new_containers = []
    self.summary_headers_num = 0
    self.summary_headers_len = 0
    self.summary_header_last_idx = None
  def get_num_piggyback_headers(self, index):
    # Compute the number of piggybacking headers that can be
    # inserted in container of a given index.
    # The following numbers are reasonable:
    # 0: 0, 1:0, ..., 4:4, ..., 8:4, ..., 16: 16, 20:4
    filtered = _last_set_bit(index)
    return (filtered | (filtered >> 1)) & 0x55555554
  def set_active(self, active):
    assert self.active == False
    self.is_active = active
  def get_next_index(self):
    index = self.next_container_idx
    self.next_container_idx += 1
    NEXT_INDEX_KEY = self._key("next_index")
    self._get_config_db()[NEXT_INDEX_KEY] = str(self.next_container_idx)
    return index
  def register_new_file(self, index):
    if index < self.next_container_idx:
      # We already have registered this container
      return
    self.new_containers.append(index)
  def process_new_files(self):
    # 1. read all new headers into loaded_headers_db.
    # 2. notify the client about the newly loaded headers.
    # 3. clean up loaded_headers_db, since these headers are
    #    no longer going to be used.
    self.new_containers.sort()
    logging.info("New containers: " + str(self.new_containers))

    self.read_summary_containers()
    self.analyze_new_containers()
  def read_summary_containers(self):
    # Load headers of all the new containers. To that end, we use the
    # piggy-backed headers found in those containers. The algorithm:
    # 1. Take last container. Read it and all the headers found in it
    # 2. Take the last container for which we don't have headers.
    # 3. Continue to step 2 if there still are unread headers.
    cur_container = self.new_containers[-1]
    while cur_container > self.next_container_idx:
      logging.info("Getting headers from container %d" % cur_container)
      container = self.storage.get_container(cur_container)
      handler = PiggybackHandler()
      container.add_handler(handler)
      container.add_handler(self.storage.get_new_container_handler())
      container.load_blocks()
      for new_index, new_header in handler.headers:
        logging.info("Container piggy-backs header %d" % new_index)
        cur_container = new_index
        self.loaded_headers_db[str(new_index)] = new_header
      self.next_container_idx = max_container_idx
  def add_summary_header(self, index, file):
    file.seek(0)
    file_contents = file.read()
    self._get_summary_headers_db()[str(index)] = file_contents
    assert len(file_contents) > 0
    self.summary_headers_len += len(str(index)) + len(file_contents)
    self.summary_headers_num += 1
    self.summary_header_last_idx = index
    if self.summary_headers_len >= self._get_container_size():
      self.write_summary_header(index)
  def write_summary(self, index):
    if self.summary_headers_num <= 1:
      print "Only one header in summary. Not writing summary header"
      return
    summary_file_name = Storage.encode_container_name(
      self.sequence_id, index, Storage.CONTAINER_EXT)
    summary_file_name_tmp = Storage.encode_container_name(
      self.sequence_id, index, Storage.CONTAINER_EXT_TMP)
    print "\nWriting summary header",
    print base64.b64encode(self.sequence_id), index,
    print "to file", summary_file_name
    tmpfile = tempfile.TemporaryFile()
    keys = []
    for key, value in self._get_summary_headers_db().iteritems():
      print "Adding header", key, len(key), len(value),
      keys.append(key)
      tmpfile.write(IE.binary_encode_int_varlen(len(key)))
      print base64.b16encode(IE.binary_encode_int_varlen(len(key))),
      print base64.b16encode(IE.binary_encode_int_varlen(len(value)))
      tmpfile.write(key)
      tmpfile.write(IE.binary_encode_int_varlen(len(value)))
      tmpfile.write(value)
    for key in keys:
      del self._get_summary_headers_db()[key]
    tmpfile.seek(0)
    self.upload_file(summary_file_name, summary_file_name_tmp, tmpfile)
    tmpfile.close()
    self.summary_headers_written += 1
    self.summary_headers_num = 0
  def _get_config_db(self):
    return self.storage.config_db
  def _get_loaded_headers_db(self):
    return self.storage.loaded_headers_db
  def _get_summary_headers_db(self):
    return self.storage.summary_headers_db
  def _get_container_size(self):
    return self.storage.container_size()
  def _key(self, suffix):
    return self.storage._key(self.sequence_id + suffix)
  def _load_container_header(self, index):
    return self.storage.load_container_header(self.sequence_id, index)
  def _load_container_header_sumary(self, index):
    # TODO: implement me
    return
