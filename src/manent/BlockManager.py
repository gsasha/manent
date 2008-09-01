#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import StringIO
import sys

import Container
import utils.IntegerEncodings as IE
import utils.Digest as Digest

# Is this block important enough (and likely small enough) to be cached always?
def is_cached(code):
  return (code != Container.CODE_DATA and
      code != Container.CODE_HEADER)
# Do we need to remember in which container this block was stored?
def is_indexed(code):
  return code != Container.CODE_HEADER

# Block Manager performs caching for different kinds of blocks.
# Some blocks (such as container ones) are stored unconditionally.
# Other blocks (data) are stored only temporarily.
class BlockManager:
  def __init__(self, db_manager, txn_handler):
    self.db_manager = db_manager
    self.txn_handler = txn_handler

    # Permanently cached blocks.
    self.cached_blocks = self.db_manager.get_database_btree("storage.db",
      "bm-cached-blocks", self.txn_handler)
    self.block_codes = self.db_manager.get_database("storage.db",
      "bm-block-types", self.txn_handler)
    # Temporarily cached blocks.
    #self.tmp_blocks = self.db_manager.get_database("storage.db",
    #  "bm-tmp-blocks", self.txn_handler)
    #self.block_longevity_data = self.db_manager.get_database("storage.db",
    #  "bm-longevity", self.txn_handler)

    # Data structure controlling the lifetime of every block. For every cached
    # block, we keep its starting epoch (when it was loaded from container or
    # created) and its longevity in epochs. Longevity grows when a block is
    # accessed.
    self.block_epoch = {}
    self.block_longevity = {}
    self.tmp_blocks = {}
    self.epoch = 0
    self.load_epoch_data()

  def close(self):
    self.save_epoch_data()

    #self.block_longevity_data.close()
    #self.tmp_blocks.close()
    self.cached_blocks.close()
    self.block_codes.close()
  #
  # Methods for the user side of the cache
  #
  def increment_epoch(self):
    # Progress the cache to a new epoch.
    self.epoch += 1
    self.cleared_blocks = []
    for digest, epoch in self.block_epoch.iteritems():
      longevity = self.block_longevity[digest]
      if epoch + longevity < self.epoch:
        self.cleared_blocks.append(digest)
    for digest in self.cleared_blocks:
      del self.block_epoch[digest]
      del self.block_longevity[digest]
      del self.tmp_blocks[digest]
  def load_epoch_data(self):
    # So far, the cache is too resource-intensive.
    # Avoid keeping it persistently.
    return
    if not self.block_longevity_data.has_key("epoch"):
      self.epoch = 0
      return
    self.epoch = int(self.block_longevity_data["epoch"])
    longevity_os = StringIO.StringIO(self.block_longevity_data["data"])
    while True:
      digest = longevity_os.read(Digest.dataDigestSize())
      if len(digest) == 0:
        break
      longevity = IE.binary_read_int_varlen(longevity_os)
      epoch = IE.binary_read_int_varlen(longevity_os)
      self.block_longevity[digest] = longevity
      self.block_epoch[digest] = epoch
  def save_epoch_data(self):
    # So far, the cache is too resource-intensive.
    # Avoid keeping it persistently until it's better optimized.
    return
    longevity_os = StringIO.StringIO()
    for digest, longevity in self.block_longevity.iteritems():
      longevity_os.write(digest)
      longevity_os.write(IE.binary_encode_int_varlen(longevity))
      epoch = self.block_epoch[digest]
      longevity_os.write(IE.binary_encode_int_varlen(epoch))
    self.block_longevity_data["data"] = longevity_os.getvalue()
    self.block_longevity_data["epoch"] = str(self.epoch)
  def add_block(self, digest, code, data):
    # Add a new block.
    if is_cached(code):
      # We store the block code only for blocks that are not DATA.
      # The DATA blocks are the majority, and so  by not storing them,
      # we save space in the database.
      self._update_cached_blocks(digest, data)
      self._update_block_codes(digest, code)
    else:
      # Storing blocks that are added is too expensive. We'll cache only those
      # that we have loaded.
      # OPTIMIZE(gsasha): this can be improved by holding in memory the blocks
      # for the last epoch and saving them once before we finish a scan. This
      # would avoid the expensive business of putting blocks to db and deleting
      # them soon thereafter.
      # self._start_block_epoch(digest)
      # self.tmp_blocks[digest] = data
      pass
  def handle_block(self, digest, code, data):
    # Handle a loaded block.
    if is_cached(code):
      self._update_cached_blocks(digest, data)
      self._update_block_codes(digest, code)
    else:
      assert code == Container.CODE_DATA
      self._start_block_epoch(digest)
      self.tmp_blocks[digest] = data
  def has_block(self, digest):
    # It is important to use block_codes here, since they are filled up
    # only when the block is saved (which is not immediate for aside
    # blocks).
    return (self.block_codes.has_key(digest) or
        self.block_epoch.has_key(digest))
  def load_block(self, digest):
    """
    Actually perform loading of the block.
    """
    logging.debug("BM Loading block " + base64.b64encode(digest) +
        " " + Container.code_name(self.get_block_code(digest)))
    if self.block_epoch.has_key(digest):
      # Block exists in the temporary cache.
      self._update_block_epoch(digest)
      return self.tmp_blocks[digest]
    if self.cached_blocks.has_key(digest):
      #
      # Blocks that sit in self.cached_blocks are never unloaded
      #
      return self.cached_blocks[digest]
    raise Exception("Block is unknown.")
  def get_block_code(self, digest):
    if self.block_codes.has_key(digest):
      return int(self.block_codes[digest])
    else:
      return Container.CODE_DATA
  def _update_cached_blocks(self, digest, data):
    if not self.cached_blocks.has_key(digest):
      self.cached_blocks[digest] = data
    # It is impossible (well, extremely implausible) that a different block
    # will have the same digest. So, we never update the cached block contents
    # if it was defined already
  def _update_block_codes(self, digest, code):
    if code != Container.CODE_DATA and not self.block_codes.has_key(digest):
      self.block_codes[digest] = str(code)
      return
    # The block code is interesting only for deciding what should be cached.
    # Otherwise, it's just informative. Besides, we write the code only for
    # non-DATA blocks, so it does not affect the caching anyway.
  def _start_block_epoch(self, digest):
    logging.debug("Starting block %s at epoch %d" %
        (base64.b64encode(digest), self.epoch))
    if self.block_epoch.has_key(digest):
      return
    self.block_epoch[digest] = self.epoch
    self.block_longevity[digest] = 4
  def _update_block_epoch(self, digest):
    assert self.block_epoch.has_key(digest)
    self.block_longevity[digest] = min(self.block_longevity[digest] * 2, 1000)
