#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import sys, os

class BlockSequencer:
  def __init__(self, db_manager, txn_manager, storage_manager):
    self.db_manager = db_manager
    self.txn_manager = txn_manager
    self.storage_manager = storage_manager
    
    # For aside blocks, we have the hashes, and keep track of the number and the
    # total size of such blocks. The data itself is stored in the BlockManager.
    self.aside_block_db = db_manager.get_database_btree(
        "storage-aside.db", "blocks", txn_manager)

    # For piggy-backed headers, we have the contents of the headers themselves.
    self.piggyback_headers_db = db_manager.get_database_btree(
        "storage-piggyback_headers.db", "blocks", txn_manager)

    self._read_vars()
    self.current_open_container = None
  def _read_vars(self):
    # Read the piggy-backing header status.
    self.piggyback_header_first = 0
    self.piggyback_header_last = -1
    if self.piggyback_headers_db.has_key("block_first"):
      self.piggyback_first_header = int(
          self.piggyback_headers_db["block_first"])
      self.piggyback_last_header = int(
          self.piggyback_headers_db["block_last"])
    # Read the aside blocks status.
    self.aside_block_first = 0
    self.aside_block_last = -1
    self.aside_block_num = 0
    self.aside_block_size = 0
    if self.aside_block_db.has_key("aside_first"):
      self.aside_block_first = int(
          self.aside_block_db["aside_first"])
      self.aside_block_last = int(
          self.aside_block_db["aside_last"])
      self.aside_block_num = int(
          self.aside_block_db["aside_num"])
      self.aside_block_size = int(
          self.aside_block_db["aside_size"])
  def _write_vars(self):
    self.piggyback_headers_db["block_first"] = str(self.piggyback_header_first)
    self.piggyback_headers_db["block_last"] = str(self.piggyback_header_last)
    self.aside_block_db["aside_first"] = str(self.aside_block_first)
    self.aside_block_db["aside_last"] = str(self.aside_block_last)
    self.aside_block_db["aside_num"] = str(self.aside_block_num)
    self.aside_block_db["aside_size"] = str(self.aside_block_size)
  def close(self):
    self._write_vars()
    self.piggyback_headers_db.close()
    self.aside_block_db.close()
  def add_block(self, digest, code, data):
    # Check if we need to put the current block aside.
    if BlockManager.is_cached(code):
      self.add_aside_block(digest, code, data)
      return
    # We're writing this block. Make sure we have a container that can accept
    # it.
    if self.current_open_container is None:
      self.current_open_container = self.open_container()
    else:
      # The container can be filled by aside data, so we might need several
      # attempts of container creation.
      while not self.current_open_container.can_add(data):
        self.write_container(self.current_open_container)
        self.current_open_container = self.open_container()
    # Ok, a container is ready.
    self.current_open_container.add_block(digest, code, data)
  def add_aside_block(self, digest, code, data):
    key = str(self.next_put_aside_block_idx)
    self.next_put_aside_block_idx += 1
    self.aside_block_db[key] = digest

    self.aside_block_num += 1
    self.aside_blocks_size += len(data)
  def write_container(self, container):
    logging.info("Finalizing container %d" % container.get_index())
    container.finish_dump()
    # 1. Get the header out of the container and store it here for
    # piggybacking.
    header_contents = container.get_header_contents()
    self.piggyback_headers_db[str(container.get_index())] = header_contents
    # 2. Ask the container to upload itself.
    container.upload()
    # 3. Let the storage manager know about the finalized container.
    self.storage_manager.container_written(container)
    self._write_vars()
  def open_container(self):
    # 1. Ask the storage to create a new empty container.
    container = self.storage_manager.get_active_storage().create_container()
    # 2. Push into the container as many piggybacking blocks as it's willing to
    # accept.
    for header in range(self.piggyback_last_header,
                        self.piggyback_first_header - 1, -1):
      header_data = self.piggyback_headers_db[str(header)]
      if not container.can_add_piggyback_header(header_data):
        break
      container.add_piggyback_header(header_data)
    # Clean up old piggyback headers
    for header in range(self.piggyback_first_header,
                        self.piggyback_last_header -
                        Container.MAX_PIGGYBACK_HEADERS):
      del self.piggyback_headers_db[str(header)]
    self.piggyback_first_header = header
    # 3. If the container can be filled by currently collected aside blocks,
    # write them out to the container, write the container out and open a new
    # one again.
    if container.is_filled_by(self.aside_block_num, self.aside_block_size):
      for block_idx in range(self.aside_block_first, self.aside_block_last):
        digest = self.aside_block_db[str(block_idx)]
        code, data = self.block_manager.load_block(digest)
        if not container.can_add(data):
          break
        container.add_block(digest, code, data)
        self.aside_block_num -= 1
        self.aside_block_size -= len(data)
      self.aside_block_first = block_idx

