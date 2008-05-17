#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import sys
import traceback

import BlockManager
import Container

class BlockSequencer:
  def __init__(self, db_manager, txn_manager, storage_manager, block_manager):
    self.db_manager = db_manager
    self.txn_manager = txn_manager
    self.storage_manager = storage_manager
    self.block_manager = block_manager
    
    # For aside blocks, we have the hashes, and keep track of the number and the
    # total size of such blocks. The data itself is stored in the BlockManager.
    self.aside_block_db = db_manager.get_database_btree(
        "tmp-aside-blocks.db", None, txn_manager)

    # For piggy-backed headers, we have the contents of the headers themselves.
    self.piggyback_headers_db = db_manager.get_database_btree(
        "tmp-piggyback-headers.db", None, txn_manager)

    self.loaded = None
    self._read_vars()
    self.current_open_container = None
    # Statistics kept to support testing.
    self.num_containers_created = 0

  def get_aside_blocks_num(self):
    return self.aside_block_num
  def get_aside_blocks_size(self):
    return self.aside_block_size
  def get_piggyback_headers_num(self):
    return self.aside_block_last + 1 - self.aside_block_first
  def _read_vars(self):
    logging.debug("BlockSequence reading vars")
    assert self.loaded is None
    self.loaded = True
    # Read the piggy-backing header status.
    self.piggyback_header_first = 0
    self.piggyback_header_last = -1
    if self.piggyback_headers_db.has_key("block_first"):
      self.piggyback_header_first = int(
          self.piggyback_headers_db["block_first"])
      self.piggyback_header_last = int(
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
    logging.debug("%d %d %d %d %d %d" %
        (self.piggyback_header_first, self.piggyback_header_last,
          self.aside_block_first, self.aside_block_last,
          self.aside_block_num, self.aside_block_size))
  def _write_vars(self):
    logging.debug("BlockSequence saving vars")
    logging.debug("%d %d %d %d %d %d" %
        (self.piggyback_header_first, self.piggyback_header_last,
          self.aside_block_first, self.aside_block_last,
          self.aside_block_num, self.aside_block_size))
    # traceback.print_stack()
    assert self.loaded is not None

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
    self.loaded = None
  def add_block(self, digest, code, data):
    # Check if we need to put the current block aside.
    if BlockManager.is_cached(code):
      self.add_aside_block(digest, code, data)
      return
    # We're writing this block. Make sure we have a container that can accept
    # it.
    if self.current_open_container is None:
      logging.debug("Creating a container for the first time")
      self.current_open_container = self.open_container()
    # The container can be filled by aside data, so we might need several
    # attempts of container creation.
    while not self.current_open_container.can_add(data):
      logging.debug("Container %d can't add data len=%d"
          % (self.current_open_container.get_index(), len(data)))
      self.write_container(self.current_open_container)
      self.current_open_container = self.open_container()
    # Ok, a container is ready.
    assert self.current_open_container.can_add(data)
    self.current_open_container.add_block(digest, code, data)
  def add_aside_block(self, digest, code, data):
    logging.debug("BlockSequencer adding aside block %s:%s:%d" %
        (base64.b64encode(digest), Container.code_name(code), len(data)))
    key = str(self.aside_block_last + 1)
    self.aside_block_last += 1
    self.aside_block_db[key] = digest

    self.aside_block_num += 1
    self.aside_block_size += len(data)
  def flush(self):
    # Write out all the aside blocks we have and clean out the last container.
    for block_idx in range(self.aside_block_first, self.aside_block_last + 1):
      digest = self.aside_block_db[str(block_idx)]
      del self.aside_block_db[str(block_idx)]
      self.aside_block_first = block_idx + 1
      code = self.block_manager.get_block_code(digest)
      data = self.block_manager.load_block(digest)
      if self.current_open_container is None:
        self.current_open_container = self.storage_manager.create_container()
        self.num_containers_created += 1
      while not self.current_open_container.can_add(data):
        self.write_container(self.current_open_container)
        self.current_open_container = self.storage_manager.create_container()
        self.num_containers_created += 1
      self.current_open_container.add_block(digest, code, data)
    if self.current_open_container is not None:
      self.write_container(self.current_open_container)
      self.current_open_container = None
    self._write_vars()
  def write_container(self, container):
    logging.debug("Finalizing container %d" % container.get_index())
    container.finish_dump()
    # 1. Get the header out of the container and store it here for
    # piggybacking.
    header_contents = container.get_header_contents()
    self.piggyback_headers_db[str(container.get_index())] = header_contents
    logging.debug("Created piggyback header %d" % container.get_index())
    self.piggyback_header_last += 1
    # 2. Ask the container to upload itself.
    container.upload()
    # 3. Let the storage manager know about the finalized container.
    self.storage_manager.container_written(container)
    self._write_vars()
    self.txn_manager.commit()
  def open_container(self):
    # 1. Ask the storage to create a new empty container.
    logging.debug("BlockSequencer: creating a new container")
    container = self.storage_manager.create_container()
    self.num_containers_created += 1
    # 2. Push into the container as many piggybacking blocks as it's willing to
    # accept.
    rejected_header = None
    logging.info("Known piggyback headers %d:%d" %
        (self.piggyback_header_first, self.piggyback_header_last))
    piggybacked_headers = []
    for header in range(self.piggyback_header_last,
                        self.piggyback_header_first - 1, -1):
      header_data = self.piggyback_headers_db[str(header)]
      if not container.can_add_piggyback_header(header_data):
        # We will do the cleaning only when the header doesn't fit in the size.
        if not container.can_add(header_data):
          logging.debug("Container %d rejected header %d size=%d:"
              " not enough space" %
              (container.get_index(), header, len(header_data)))
          rejected_header = header
        break
      piggybacked_headers.append(header)
      logging.debug("Adding piggyback header %d to container %d"
          % (header, container.get_index()))
      container.add_piggyback_header(header, header_data)
    # Clean up piggyback headers that cannot be inserted anymore.
    if rejected_header is not None:
      for header in range(self.piggyback_header_first, rejected_header + 1):
        logging.debug("Deleting unusable piggyback header %d" % header)
        del self.piggyback_headers_db[str(header)]
      self.piggyback_first_header = rejected_header + 1
    # 3. If the container can be filled by currently collected aside blocks,
    # write them out to the container, write the container out and open a new
    # one again.
    nondata_blocks_added = 0
    if container.is_filled_by(self.aside_block_num, self.aside_block_size):
      logging.debug("Adding aside blocks %d:%d to container %d" %
          (self.aside_block_first, self.aside_block_last,
            container.get_index()))
      for block_idx in range(self.aside_block_first, self.aside_block_last + 1):
        digest = self.aside_block_db[str(block_idx)]
        code = self.block_manager.get_block_code(digest)
        data = self.block_manager.load_block(digest)
        if not container.can_add(data):
          logging.debug("Container %d cannot add aside block %d size=%d" %
              (container.get_index(), block_idx, len(data)))
          break
        logging.debug("Adding aside block %d to container %d"
            % (block_idx, container.get_index()))
        del self.aside_block_db[str(block_idx)]
        container.add_block(digest, code, data)
        nondata_blocks_added += 1
        self.aside_block_num -= 1
        self.aside_block_size -= len(data)
        self.aside_block_first = block_idx + 1
    logging.info("Container %d nondata blocks:%d, piggyback headers:%s" %
        (container.get_index(), nondata_blocks_added, str(piggybacked_headers)))
    return container

