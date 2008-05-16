#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import sys

import Container

def is_cached(code):
  return (code != Container.CODE_DATA and
      code != Container.CODE_HEADER)

class BlockManager:
  def __init__(self, db_manager, txn_handler):
    self.db_manager = db_manager
    self.txn_handler = txn_handler

    # These two databases are scratch-only, so they don't need to reliably
    # survive through program restarts
    self.requested_blocks = self.db_manager.get_scratch_database(
      "scratch-requested-blocks.db", None)
    self.loaded_blocks = self.db_manager.get_scratch_database(
      "scratch-data-blocks.db", None)
    self.cached_blocks = self.db_manager.get_database("cached-blocks.db",
      None, self.txn_handler)
    self.block_codes = self.db_manager.get_database("block-types.db",
      None, self.txn_handler)
    #
    # It is possible that the program was terminated before the scratch
    # cache was removed. In that case, it contains junk data
    #
    self.requested_blocks.truncate()
    self.loaded_blocks.truncate()
  def close(self):
    self.requested_blocks.close()
    self.loaded_blocks.close()
    self.cached_blocks.close()
    self.block_codes.close()
  #
  # Methods for the user side of the cache
  #
  def request_block(self, digest):
    """
    Used for preprocessing, to make all the future needed blocks known -
    this is to avoid reloading containers unnecessarily.
    """
    #print "Requested block:", base64.b64encode(digest)
    if self.requested_blocks.has_key(digest):
      self.requested_blocks[digest] = str(
        int(self.requested_blocks[digest]) + 1)
    else:
      self.requested_blocks[digest] = "1"
  def is_requested(self, digest):
    return self.requested_blocks.has_key(digest)
  def add_block(self, digest, code, data):
    if code != Container.CODE_DATA:
      # We store the block code only for blocks that are not DATA.
      # The DATA blocks are the majority, and so  by not storing them,
      # we save space in the database.
      self._update_cached_blocks(digest, data)
      self._update_block_codes(digest, code)
  def handle_block(self, digest, code, data):
    if is_cached(code):
      self._update_cached_blocks(digest, data)
      self._update_block_codes(digest, code)
    else:
      assert code == Container.CODE_DATA
      if self.requested_blocks.has_key(digest):
        # This happens during loading, not scan, so no real need to protect.
        self.loaded_blocks[digest] = data
  def has_block(self, digest):
    # It is important to use block_codes here, since they are filled up
    # only when the block is saved (which is not immediate for aside
    # blocks).
    return (self.block_codes.has_key(digest) or
        self.loaded_blocks.has_key(digest))
  def load_block(self, digest):
    """
    Actually perform loading of the block. Assumes that the block
    was reported by request_block, and was loaded not more times than
    it was requested.
    """
    logging.debug("BM Loading block " + base64.b64encode(digest) +
        " " + Container.code_name(self.get_block_code(digest)))
    if self.cached_blocks.has_key(digest):
      #
      # Blocks that sit in self.cached_blocks are never unloaded
      #
      return self.cached_blocks[digest]
    if self.loaded_blocks.has_key(digest):
      data = self.loaded_blocks[digest]
      #
      # See if we can unload this block
      #
      if self.requested_blocks.has_key(digest):
        refcount = int(self.requested_blocks[digest]) - 1
        if refcount == 0:
          logging.debug("Removing block from loaded " +
              base64.b64encode(digest))
          del self.requested_blocks[digest]
          del self.loaded_blocks[digest]
        else:
          self.requested_blocks[digest] = str(refcount)
      return data
    raise Exception("Block neither cached nor loaded!!!")
  def get_block_code(self, digest):
    if self.block_codes.has_key(digest):
      return int(self.block_codes[digest])
    else:
      return Container.CODE_DATA
  def _update_cached_blocks(self, digest, data):
    if not self.cached_blocks.has_key(digest):
      self.cached_blocks[digest] = data
    # It is impossible (well, extremely implaausible) that a different block
    # will have the same digest. So, we never update the cached block contents
    # if it was defined already
  def _update_block_codes(self, digest, code):
    if code != Container.CODE_DATA and not self.block_codes.has_key(digest):
      self.block_codes[digest] = str(code)
      return
    # The block code is interesting only for deciding what should be cached.
    # Otherwise, it's just informative. Besides, we write the code only for
    # non-DATA blocks, so it does not affect the caching anyway.
