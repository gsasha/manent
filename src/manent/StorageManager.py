#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import cStringIO as StringIO
import time

import BlockManager
import BlockSequencer
import Container
import Reporting
import Storage
import utils.Digest as Digest
import utils.IntegerEncodings as IE

PREFIX = "STORAGE_MANAGER."

logger_sm = logging.getLogger("manent.storage_manager")

#--------------------------------------------------------
# Utility methods
#--------------------------------------------------------
def _encode_block_info(seq_idx, container_idx):
  io = StringIO.StringIO()
  io.write(IE.binary_encode_int_varlen(seq_idx))
  io.write(IE.binary_encode_int_varlen(container_idx))
  return io.getvalue()
def _decode_block_info(encoded):
  io = StringIO.StringIO(encoded)
  seq_idx = IE.binary_read_int_varlen(io)
  container_idx = IE.binary_read_int_varlen(io)
  return (seq_idx, container_idx)

class StorageManager:
  """Handles the moving of blocks to and from storages.

  Input: a stream of blocks
  Creates containers and sends them to storage
  
  Data structure:
  block_container_db keeps for each hash the list of sequence_id+container_id
  
  aside_block_db holds the digests for blocks that have been set aside.
  The blocks themselves are supposed to be stored by the block manager.

  The sequences are numbered globally for all the storages, and are identified
  by a random sequence id generated by the storage automatically
  
  seq_to_index keeps for each sequence id its storage idx an global sequence idx
  index_to_seq keeps for each global sequence idx its storage idx and
  sequence id. The information is encoded in config_db.
  
  storage idxs are stored in the config_db["storage_idxs"]
  """
  def __init__(self, db_manager, txn_manager):
    self.db_manager = db_manager
    self.txn_manager = txn_manager
    self.block_manager = BlockManager.BlockManager(
        self.db_manager, self.txn_manager)
    self.block_sequencer = BlockSequencer.BlockSequencer(
        self.db_manager, self.txn_manager, self, self.block_manager)
    self.report_manager = Reporting.DummyReportManager()
    self.num_new_blocks_reporter = Reporting.DummyReporter()
    self.size_new_blocks_reporter = Reporting.DummyReporter()

    self.config_db = db_manager.get_database_btree("config.db", "storage",
      txn_manager)
    logging.debug("Loaded storage manager db")
    for key, val in self.config_db.iteritems():
      logging.debug("Storage manager db: [%s]->[%s]" %
          (base64.b64encode(key),
            (base64.b64encode(val))))
    self.block_container_db = db_manager.get_database_hash("storage.db",
      "blocks", txn_manager)
    logging.debug("********** Loaded storage manager logs")
    for key, val in self.config_db.iteritems():
      logging.debug("Storage manager blocks: [%s]->[%s]" %
          (base64.b64encode(key), base64.b64encode(val)))

    # Mapping of storage sequences to indices and vice versa
    # The storage sequence data consists of storage index and sequence
    # ID string
    # In the config_db we store the persistent copy of the information
    # in the seq_to_index and index_to_seq:
    # repo.%index.seq = sequence
    # repo.%index.storage = storage index
    # repo.next_index = <the next index>
    self.seq_to_index = {}
    self.index_to_seq = {}
    NS_KEY = self._key("next_seq")
    if self.config_db.has_key(NS_KEY):
      self.next_seq_idx = int(self.config_db[NS_KEY])
    else:
      self.next_seq_idx = 0
    SEQ_PREFIX = self._key("SEQ.")
    for key, val in self.config_db.iteritems_prefix(SEQ_PREFIX):
      sequence_id = key[len(SEQ_PREFIX):]
      storage_idx, sequence_idx = IE.binary_decode_int_varlen_list(val)
      self.seq_to_index[sequence_id] = (storage_idx, sequence_idx)
      self.index_to_seq[sequence_idx] = (storage_idx, sequence_id)
    self.block_listeners = []
  def set_report_manager(self, report_manager):
    self.report_manager = report_manager
    self.num_new_blocks_reporter = report_manager.find_reporter(
        "scan.counts.new_blocks", 0)
    self.size_new_blocks_reporter = report_manager.find_reporter(
        "scan.counts.size_new_blocks", 0)
  def close(self):
    for index, storage in self.storages.iteritems():
      storage.close()
    self.block_container_db.close()
    self.config_db.close()
    self.block_sequencer.close()
    self.block_manager.close()
  class BlockScanningListener:
    """This listener is used for loading blocks in new containers."""
    def __init__(self, storage_manager, storage_idx):
      self.storage_manager = storage_manager
      self.storage_idx = storage_idx
    def is_requested(self, sequence_id, container_idx, digest, code):
      # See if this block belongs to a previously unseen sequence.
      if not self.storage_manager.has_sequence(sequence_id):
        self.storage_manager.register_sequence(self.storage_idx, sequence_id)
      storage_idx, sequence_idx = self.storage_manager.seq_to_index[sequence_id]
      # Record to which container does this block belong.
      encoded = _encode_block_info(sequence_idx, container_idx)
      if not self.storage_manager.block_container_db.has_key(digest):
        if BlockManager.is_indexed(code):
          self.storage_manager.block_container_db[digest] = encoded
      # Check if we want the data of this block.
      return BlockManager.is_cached(code)
    def loaded(self, sequence_id, digest, code, data):
      self.storage_manager.block_manager.handle_block(digest, code, data)
  def add_block_listener(self, listener):
    self.block_listeners.append(listener)
  def create_block_listener(self, storage_idx):
    listener = StorageManager.BlockScanningListener(self, storage_idx)
    if self.block_listeners == []:
      return listener

    class CompositeBlockListener:
      def __init__(self):
        self.listeners = []
      def add_listener(self, listener):
        self.listeners.append(listener)
      def is_requested(self, sequence_id, container_idx, digest, code):
        result = False
        for l in self.listeners:
          if l.is_requested(sequence_id, container_idx, digest, code):
            result = True
        return result
      def loaded(self, sequence_id, digest, code, data):
        for l in self.listeners:
          l.loaded(sequence_id, digest, code, data)

    composed_listener = CompositeBlockListener()
    for l in self.block_listeners:
      composed_listener.add_listener(l)
    composed_listener.add_listener(listener)
    return composed_listener
  def _key(self, suffix):
    return PREFIX + suffix

  def has_sequence(self, sequence_id):
    return self.seq_to_index.has_key(sequence_id)
  def register_sequence(self, storage_idx, sequence_id):
    # Generate new index for this sequence
    logger_sm.debug("new sequence detected in storage %d: %s" %
      (storage_idx, base64.urlsafe_b64encode(sequence_id)))
    sequence_idx = self.next_seq_idx
    self.next_seq_idx += 1
    self.config_db[self._key("next_seq")] = str(self.next_seq_idx)
    self.config_db[self._key("SEQ." + sequence_id)] = \
      IE.binary_encode_int_varlen_list([storage_idx, sequence_idx])
      
    self.seq_to_index[sequence_id] = (storage_idx, sequence_idx)
    self.index_to_seq[sequence_idx] = (storage_idx, sequence_id)
  def add_storage(self, storage_params):
    # When we add a storage, the following algorithm is executed:
    # 1. If the storage is already in the shared db, it is just added
    # 2. If the storage is not in the shared db, the storage location
    #    is rescanned. All storage locations found there are added as
    #    base storages, and a new one is created.
    storage_idx = self.assign_storage_idx()

    listener = self.create_block_listener(storage_idx)
    storage = Storage.create_storage(self.db_manager, self.txn_manager,
      storage_idx, storage_params, listener)
    storage.set_report_manager(self.report_manager)
    self.storages[storage_idx] = storage
    return storage_idx
  def load_storages(self):
    #
    # All storages except for the specified one are inactive, i.e., base.
    # Inactive storages can be used to pull data blocks from, and must
    # be updated on each invocation, since somebody else might be adding
    # blocks there
    #
    logging.debug("StorageManager loading storages")
    self.storages = {}
    self.active_storage_idx = None
    for storage_idx in self.get_storage_idxs():
      logging.debug("StorageManager loading storage %d" % storage_idx)
      handler = self.create_block_listener(storage_idx)
      storage = Storage.load_storage(self.db_manager, self.txn_manager,
        storage_idx, handler)
      storage.set_report_manager(self.report_manager)
      self.storages[storage_idx] = storage
      if storage.is_active():
        logging.debug("Storage is active")
        seq_id = storage.get_active_sequence_id()
        self.active_storage_idx, seq_idx = self.seq_to_index[seq_id]

  def get_storage_idxs(self):
    KEY = self._key("storage_idxs")
    if not self.config_db.has_key(KEY):
      logging.debug("--- Storage manager knows of no storage idxs")
      return []
    idxs_str = self.config_db[KEY]
    storage_idxs = IE.binary_decode_int_varlen_list(idxs_str)
    logging.debug("--- Storage manager knows of idxs: %s" % str(storage_idxs))
    return storage_idxs
  def assign_storage_idx(self):
    storage_idxs = self.get_storage_idxs()
    if storage_idxs == []:
      storage_idx = 0
    else:
      storage_idx = max(storage_idxs) + 1
    idxs_str = IE.binary_encode_int_varlen_list(storage_idxs + [storage_idx])
    self.config_db[self._key("storage_idxs")] = idxs_str
    return storage_idx
  def get_storage_config(self, storage_index):
    return self.storages[storage_index].get_config()
  def make_active_storage(self, storage_index):
    if self.active_storage_idx is not None:
      raise Exception("Switching active storage not supported yet")
    storage = self.storages[storage_index]
    seq_id = storage.create_sequence()
    self.register_sequence(storage_index, seq_id)
    self.active_storage_idx = storage_index
  def get_active_sequence_id(self):
    storage = self.storages[self.active_storage_idx]
    return storage.get_active_sequence_id()
  def get_active_storage_index(self):
    return self.active_storage_idx
  def get_block_size(self):
    storage = self.storages[self.active_storage_idx]
    return storage.get_block_size()
  def add_block(self, digest, code, data):
    self.block_manager.add_block(digest, code, data)

    if self.block_container_db.has_key(digest):
      return False

    self.num_new_blocks_reporter.increment(1)
    self.size_new_blocks_reporter.increment(len(data))
    logging.debug("Storage manager adding new block %s:%s:%d" %
        (base64.b64encode(digest), Container.code_name(code), len(data)))
    self.block_sequencer.add_block(digest, code, data)
    return True
  def load_block(self, digest):
    logging.debug("SM loading block " + base64.b64encode(digest))
    if not self.block_manager.has_block(digest):
      logging.debug("loading blocks for" + base64.b64encode(digest))

      sequence_idx, container_idx = _decode_block_info(
        self.block_container_db[digest])
      storage_idx, sequence_id = self.index_to_seq[sequence_idx]
      storage = self.storages[storage_idx]

      logging.debug("Digest %s is in %d:%s:%d" %
          (base64.b64encode(digest), sequence_idx,
          base64.urlsafe_b64encode(sequence_id), container_idx))

      self.report_manager.increment(
          "storage.container.download.%s.%d.data.digest" % (
            base64.b64encode(sequence_id),
            container_idx),
          1)
      start_time = time.time()

      container = storage.get_container(sequence_id, container_idx)
      self.block_manager.increment_epoch()
      container.load_blocks(self.block_manager.get_listener())

      self.report_manager.append(
          "storage.container.download.%s.%d.data.time" % (
            base64.b64encode(sequence_id),
            container_idx),
          time.time() - start_time)
    return self.block_manager.load_block(digest)
  def load_blocks_for(self, digest, handler):
    # This method exists only for testing.
    logging.debug("SM loading blocks for " + base64.b64encode(digest))
    sequence_idx, container_idx = _decode_block_info(
        self.block_container_db[digest])
    storage_idx, sequence_id = self.index_to_seq[sequence_idx]
    storage = self.storages[storage_idx]
    container = storage.get_container(sequence_id, container_idx)
    container.load_blocks(handler)

  def get_block_code(self, digest):
    logging.debug("SM getting code for " + base64.b64encode(digest))
    return self.block_manager.get_block_code(digest)
  def flush(self):
    self.block_sequencer.flush()
    # All storages must flush, because they might have found new containers.
    for storage in self.storages.itervalues():
      storage.flush()
  def create_container(self):
    storage = self.storages[self.get_active_storage_index()]
    return storage.create_container()
  def container_written(self, container):
    # Update the container in the blocks db
    container_idx = container.get_index()
    storage_idx, seq_idx = self.seq_to_index[container.get_sequence_id()]
    encoded = _encode_block_info(seq_idx, container_idx)
    for digest, code in container.list_blocks():
      self.block_container_db[digest] = encoded
    self.block_manager.increment_epoch()
    self.txn_manager.commit()
