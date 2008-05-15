#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import re

import Increment
import utils.IntegerEncodings as IE

# TODO: reconstruction of IncrementManager

class IncrementManager:
  def __init__(self, db_manager, txn_handler, storage_manager):
    self.storage_manager = storage_manager
    self.config_db = db_manager.get_database_btree("config.db", "increments",
      txn_handler)
    assert self.config_db is not None
    
    self.active_increment = None
    self.previous_increment = None

  def close(self):
    self.config_db.close()

  def get_increments(self):
    increments = {}

    increment_rexp = re.compile('Increment\.([^\.]+)\.([^\.]+)')
    for key, value in self.config_db.iteritems_prefix("Increment"):
      if key.endswith("fs_digest"):
        match = increment_rexp.match(key)
        storage_index = IE.ascii_decode_int_varlen(match.group(1))
        index = IE.ascii_decode_int_varlen(match.group(2))

        if not increments.has_key(storage_index):
          increments[storage_index] = []
        increments[storage_index].append(index)
    
    return increments

  def start_increment(self, comment):
    assert self.active_increment is None

    increments = self.get_increments()
    #
    # Create the new active increment
    #
    storage_index = self.storage_manager.get_active_storage_index()
    if increments.has_key(storage_index):
      last_index = sorted(increments[storage_index])[-1]
      next_index = last_index + 1
    else:
      last_index = None
      next_index = 0

    self.active_increment = Increment.Increment(self.storage_manager,
        self.config_db)
    self.active_increment.start(storage_index, next_index, comment)

    if last_index is None:
      return (None, None)

    last_increment = Increment.Increment(self.storage_manager, self.config_db)
    last_increment.load(storage_index, last_index)
    return (last_increment.get_fs_digest(), last_increment.get_fs_level())

  def get_increment(self, storage_idx, index):
    increment = Increment.Increment(self.storage_manager, self.config_db)
    increment.load(storage_idx, index)
    return increment

  def finalize_increment(self, digest, level):
    assert self.active_increment is not None

    logging.info("Finalizing increment %d to %s" %
        (self.active_increment.index, base64.b64encode(digest)))
    inc_digest = self.active_increment.finalize(digest, level)
    self.active_increment = None
    return inc_digest

  def reconstruct(self):
    class Handler:
      """Handler reports all increment-related data"""
      def __init__(self, increment_manager):
        self.increment_manager = increment_manager
      def block_loaded(self, digest, data, code):
        if code != CODE_INCREMENT_DESCRIPTOR:
          return
        increment = Increment.Increment(self.increment_manager,
          self.increment_manager.block_manager, self.increment_manager.db)
        increment.reconstruct(digest)
    
    handler = Handler(self)
    self.storage_manager.reconstruct(handler)
