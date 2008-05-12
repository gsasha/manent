#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import cStringIO as StringIO
import logging
import os
import re
import shutil
import tempfile
import traceback

import Config
import Container
import utils.IntegerEncodings as IE
import Storage

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
  def set_active(self, active):
    assert self.active == None
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
  def get_new_containers(self):
    return self.new_containers
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
