#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import sys, os

FIRST_BLOCK_KEY = "FIRST_BLOCK"
LAST_BLOCK_KEY = "LAST_BLOCK"

class BlockSequencer:
  def __init__(self, db_manager, txn_manager, storage_manager):
    self.db_manager = db_manager
    self.txn_manager = txn_manager
    self.storage_manager = storage_manager
    
    # Initialize the aside block logic
    self.aside_block_db = db_manager.get_database_btree(
        "storage-aside.db", "blocks", txn_manager)

    # Initialize the header piggy-backing logic
    self.piggyback_headers_db = db_manager.get_database_btree(
        "storage-piggyback_headers.db", "blocks", txn_manager)
    self.piggyback_first_header = 0
    self.piggyback_last_header = -1
    if self.piggyback_headers_db.has_key(FIRST_BLOCK_KEY):
      self.piggyback_first_header = int(
          self.piggyback_headers_db[FIRST_BLOCK_KEY])
      self.piggyback_last_header = int(
          self.piggyback_headers_db[LAST_BLOCK_KEY])

    self.current_open_container = None
  def close(self):
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

    self.num_aside_blocks += 1
    self.size_aside_blocks += len(data)
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
    pass
