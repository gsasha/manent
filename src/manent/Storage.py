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
import time
import traceback

import Config
import Container
import utils.IntegerEncodings as IE
import utils.RemoteFSHandler as RemoteFSHandler
import Reporting

CONTAINER_EXT = "mf"
CONTAINER_EXT_TMP = "mf-tmp"

def _instantiate(storage_type, storage_params):
  if storage_type == "directory":
    return DirectoryStorage(storage_params)
  elif storage_type == "mail":
    return MailStorage(storage_params)
  elif storage_type == "ftp":
    return FTPStorage(storage_params, RemoteFSHandler.FTPHandler)
  elif storage_type == "sftp":
    return FTPStorage(storage_params, RemoteFSHandler.SFTPHandler)
  elif storage_type == "__mock__":
    return MemoryStorage(storage_params)
  raise Exception("Unknown storage_type type" + storage_type)

# Create a storage with given parameters
def create_storage(db_manager, txn_manager, index, params,
           new_block_handler):
  logging.debug("Opening storage database %s:%s" %
      ("config.db", "storage.%d" % index))
  config_db = db_manager.get_database_btree("config.db",
    "storage.%d" % index, txn_manager)
  storage_type = params['type']
  config_db["TYPE"] = storage_type
  storage_params = StorageParams(index, db_manager, txn_manager, config_db)
  storage = _instantiate(storage_type, storage_params)
  storage.configure(params, new_block_handler)
  return storage

def load_storage(db_manager, txn_manager, index, new_block_handler):
  logging.debug("Opening storage database %s:%s" %
      ("config.db", "storage.%d" % index))
  config_db = db_manager.get_database_btree("config.db",
    "storage.%d" % index, txn_manager)
  storage_type = config_db["TYPE"]
  storage_params = StorageParams(index, db_manager, txn_manager, config_db)
  storage = _instantiate(storage_type, storage_params)
  storage.load_configuration(new_block_handler)
  return storage


# Configuration parameters for the Storage base class.
# Designed to be passed to any descendant of Storage,
# so that params can be changed orthogonally to the number of storages
class StorageParams:
  def __init__(self, index, db_manager, txn_manager, config_db):
    self.index = index
    self.db_manager = db_manager
    self.txn_manager = txn_manager
    self.config_db = config_db


class Storage:
  def __init__(self, params):
    self.db_manager = params.db_manager
    self.txn_manager = params.txn_manager
    self.config_db = params.config_db
    params.config_db = None
    self.loaded_headers_db = params.db_manager.get_scratch_database(
      "scratch_loaded_headers_%d.db" % params.index, None)
    self.index = params.index
    self.sequence_next_container = {}
    self.active_sequence_id = None
    # For statistics and testing
    self.headers_loaded_total = 0
    self.headers_loaded_from_summary = 0
    self.headers_loaded_from_storage = 0

    self.report_manager = Reporting.DummyReportManager()

  def set_report_manager(self, report_manager):
    self.report_manager = report_manager

  def close(self):
    self.loaded_headers_db.close()
    self.loaded_headers_db = None
    self.config_db.close()
    self.config_db = None

  def _key(self, suffix):
    return "%s" % (suffix)
  #
  # Loading
  #
  def configure(self, config, new_block_handler):
    for key, val in config.iteritems():
      self.config_db[self._key('CONFIG.' + key)] = val
      logging.debug("setting config_db[%s]=%s" %
          (self._key('CONFIG.'+key), val))
    
    self.config = config
    self.load_sequences(new_block_handler)
  def load_configuration(self, new_block_handler):
    self.config = self.get_config()
    self.load_sequences(new_block_handler)
  def get_config(self):
    PREFIX = self._key('CONFIG.')
    PREFIX_len = len(PREFIX)
    config = {}
    for key, val in self.config_db.iteritems_prefix(PREFIX):
      config[key[PREFIX_len:]] = val
    return config

  def get_encryption_key(self):
    if self.config.has_key('encryption_key'):
      return self.config['encryption_key']
    return None
  def get_index(self):
    return self.index

  # Data structure stored in a database for a specific storage:
  # active_sequence - the sequence to which new containers
  #                   are added
  # next_container.$sequence - the index of the last known
  #     container in the given sequence. Used to determine which new
  #     containers have appeared for the sequence. Also, used to know if this
  #     sequence has been loaded already.
  def create_sequence(self, test_override_sequence_id=None):
    # If a sequence is created, rather than discovered, it can be done only
    # in order to make it active one.
    assert self.active_sequence_id is None
    if test_override_sequence_id:
      # This is used in testing only
      self.active_sequence_id = test_override_sequence_id
    else:
      self.active_sequence_id = os.urandom(12)

    logging.debug("Creating sequence %s" %
        base64.b64encode(self.active_sequence_id))
    self.sequence_next_container[self.active_sequence_id] = 0
    self.config_db[self._key("active_sequence")] = self.active_sequence_id
    self.config_db[self._key("next_container.%s" % self.active_sequence_id)] =\
        str(self.sequence_next_container[self.active_sequence_id])
    return self.active_sequence_id
  def is_active(self):
    return self.active_sequence_id is not None
  def get_active_sequence_id(self):
    return self.active_sequence_id
  def get_next_index(self):
    return self.sequences[self.active_sequence_id].get_next_index()
  def load_sequences(self, new_block_handler):
    logging.debug("Loading sequences for storage %d" % self.index)
    # Load previously known sequences
    AS_KEY = self._key("active_sequence")
    if self.config_db.has_key(AS_KEY):
      self.active_sequence_id = self.config_db[AS_KEY]
    
    SEQ_PREFIX = self._key("next_container.")
    for key, value in self.config_db.iteritems_prefix(SEQ_PREFIX):
      seq_id = key[len(SEQ_PREFIX):]
      self.sequence_next_container[seq_id] = int(value)

    # Load the data from the storage location
    sequence_new_containers = {}
    sequence_computed_next_container = {}
    try:
      container_files = self.list_container_files()
    except:
      logging.info("Failed to fetch the container files."
          " Probably a network problem")
      container_files = []
    for name in container_files:
      sequence_id, index, extension = decode_container_name(name)
      if extension != CONTAINER_EXT:
        # This is not a Manent container file.
        continue
      if not self.sequence_next_container.has_key(sequence_id):
        logging.info("Found new sequence %s " %
          base64.b64encode(sequence_id))
        self.sequence_next_container[sequence_id] = 0
      if not sequence_new_containers.has_key(sequence_id):
        sequence_new_containers[sequence_id] = []
      if index >= self.sequence_next_container[sequence_id]:
        sequence_new_containers[sequence_id].append(index)

    for sequence_id, containers in sequence_new_containers.iteritems():
      if (sequence_id == self.active_sequence_id and
          containers != []):
         # TODO(gsasha): Instead of crashing, abort this sequence and start a new one.
         # We have seen that somebody else has added a container into sequence we're
         # supposed to be writing exclusively.
         raise Exception("Unexpected new containers %s in sequence %s",
                         (", ".join([str(i) for i in containers]),
                          base64.b64encode(sequence_id)))
      containers.sort()
      logging.debug("New containers in sequence %s: %s" %
          (base64.urlsafe_b64encode(sequence_id), str(containers)))
      if containers != []:
        self.sequence_next_container[sequence_id] = max(containers) + 1

    # Process the new containers
    # 1. Read the new containers that have piggyback headers - this way we get
    # all the headers without actually reading them.
    logging.debug("Loading sequences for storage %d: "
        "reading new containers for headers" %
        self.index)
    for sequence_id, containers in sequence_new_containers.iteritems():
      # We process files in the reverse order because that's how piggybacking
      # headers are organized. This necessarily converges quickly to reading
      # from "full" containers.
      containers.sort()
      # We are interested only in loading containers that are summary (a
      # finished increment should add a summary container). So, we discard up to
      # 3 containers until we find a summary one.
      while len(containers) != 0 and (containers[-1] + 1) % 4 != 0:
        containers.pop()
      containers.reverse()
      loaded_containers = {} 
      for index in containers:
        KEY = sequence_id + str(index)
        if self.loaded_headers_db.has_key(KEY):
          logging.debug("Skipping container %d: header piggybacked" % index)
          continue
        # If a summary container gets full before it can accomodate all the
        # appointed headers, it can make us try to load a container that is
        # non-summary. We want to avoid this, and thus we walk upwards to find a
        # container which is actually a summary (i.e., contains no data blocks).
        # There is a danger of infinite looping if we somehow get to a header
        # that fills a container single-handedly, but I think that is highly
        # unlikely and won't handle it for now.
        while ((index + 1) % 4 != 0 and (index + 1) in containers and
            not self.loaded_headers_db.has_key(KEY)):
          logging.debug("Do not want to read non-piggyback container %d" %
              index)
          index += 1
        logging.info("Reading container %s:%d for piggybacked headers" %
            (base64.urlsafe_b64encode(sequence_id), index))
        container = self.get_container(sequence_id, index)
        loaded_containers[index] = 1
        class PiggybackHeaderLoadHandler:
          """Record all the piggybacked headers reported by the container.
          Ask the incoming handler for all the other blocks.
          Note that the incoming handler receives the sequence id along with
          each block."""
          def __init__(self, sequence_id, container_idx, block_handler):
            self.sequence_id = sequence_id
            self.container_idx = container_idx
            self.block_handler = block_handler
            self.headers = {}
          def is_requested(self, digest, code):
            logging.debug("Seeing block code=%s, digest=%s" %
                (Container.code_name(code), base64.b64encode(digest)))
            if self.block_handler.is_requested(
                self.sequence_id, self.container_idx, digest, code):
              return True
            logging.debug("Seeing block of code %s" % Container.code_name(code))
            return code == Container.CODE_HEADER
          def loaded(self, digest, code, data):
            if self.block_handler.is_requested(
                self.sequence_id, self.container_idx, digest, code):
              self.block_handler.loaded(sequence_id, digest, code, data)
            if code == Container.CODE_HEADER:
              index = Container.decode_piggyback_container_index(digest)
              self.headers[index] = data
        pb_handler = PiggybackHeaderLoadHandler(
            sequence_id, index, new_block_handler)

        self.report_manager.increment(
            "storage.container.download.%s.%d.piggyback_header.count" % (
              base64.b64encode(sequence_id),
              index),
            1)
        start_time = time.time()

        container.load_blocks(pb_handler)
        logging.info("Container %s:%d piggybacks headers %s" %
            (base64.urlsafe_b64encode(sequence_id), index,
              str(sorted(pb_handler.headers.keys()))))
        self.report_manager.append(
            "storage.container.download.%s.%d.piggyback_header.time" % (
              base64.b64encode(sequence_id),
              index),
            time.time() - start_time)
        for index, header in pb_handler.headers.iteritems():
          KEY = sequence_id + str(index)
          self.loaded_headers_db[KEY] = header
      # Make sure that we will not try to re-load the containers we have loaded
      # already.
      filtered_containers = [c for c in containers if not
          loaded_containers.has_key(c)]
      sequence_new_containers[sequence_id] = filtered_containers
    # 2. Read all the new containers (except for those that we have read
    # already). Since we have loaded all the headers already, there is no
    # network use for containers that contain only DATA blocks.
    logging.debug("Loading sequences for storage %d:"
        "reading new containers for blocks" %
        self.index)
    for sequence_id, containers in sequence_new_containers.iteritems():
      containers.sort()
      for index in containers:
        logging.info("Reading container %d for metadata blocks" % index)
        container = self.get_container(sequence_id, index)
        class BlockLoadHandler:
          """Transfer all the incoming blocks to the given handler,
          adding the sequence id to each of them."""
          def __init__(self, sequence_id, container_idx, block_handler):
            self.sequence_id = sequence_id
            self.container_idx = container_idx
            self.block_handler = block_handler
          def is_requested(self, digest, code):
            if self.block_handler.is_requested(
                self.sequence_id, self.container_idx, digest, code):
              return True
            return False
          def loaded(self, digest, code, data):
            if self.block_handler.is_requested(
                self.sequence_id, self.container_idx, digest, code):
              self.block_handler.loaded(self.sequence_id, digest, code, data)
        handler = BlockLoadHandler(sequence_id, index, new_block_handler)
        self.report_manager.increment(
            "storage.container.download.%s.%d.metadata.count" % (
              base64.b64encode(sequence_id),
              index),
            1)
        start_time = time.time()

        container.load_blocks(handler)

        self.report_manager.append(
            "storage.container.download.%s.%d.metadata.time" % (
              base64.b64encode(sequence_id),
              index),
            time.time() - start_time)
      self.txn_manager.checkpoint()
    # 3. Update the next_container information for all the sequences.
    logging.debug("Loading sequences for storage %d:"
        "updating container information" %
        self.index)
    for sequence_id, next_container in self.sequence_next_container.iteritems():
      logging.debug("Sequence %s next_container:%d" %
          (base64.b64encode(sequence_id), next_container))
      self.config_db["next_container." + sequence_id] = str(next_container)
    self.loaded_headers_db.truncate()
    self.txn_manager.commit()
  def flush(self):
    # TODO(gsasha): implement this
    pass
  def info(self):
    pass

  #
  # Container management
  #
  def create_container(self):
    if self.active_sequence_id is None:
      raise Exception("Can't create a container for an inactive storage")
    container = Container.Container(self)
    index = self.sequence_next_container[self.active_sequence_id]
    container.start_dump(self.active_sequence_id, index)

    self.sequence_next_container[self.active_sequence_id] = index + 1
    self.config_db["next_container." + self.active_sequence_id] = str(index + 1)
    return container
  def get_container(self, sequence_id, index):
    container = Container.Container(self)
    container.start_load(sequence_id, index)
    return container

  def load_header_file(self, sequence_id, index):
    self.headers_loaded_total += 1
    # TODO(gsasha): MULTITHREADED we can't read several sequences of the same
    # storage concurrently since loaded headers of different sequences would
    # conflict.
    KEY = sequence_id + str(index)
    if self.loaded_headers_db.has_key(KEY):
      stream = StringIO.StringIO(self.loaded_headers_db[KEY])
      del self.loaded_headers_db[KEY]
      self.headers_loaded_from_summary += 1
      return stream
    self.headers_loaded_from_storage += 1
    return None
  def load_body_file(self, sequence_id, index):
    raise Exception("load_body_file is abstract")
  def upload_container(self, sequence_id, index, header_file, body_file):
    raise Exception("upload_container is abstract")
  
  #
  # Block parameters
  #
  def get_block_size(self):
    return 256*1024
  
# Used only for testing
class MemoryStorage(Storage):
  # NOTE: global var. It doesn't matter, since it's for testing only.
  files = {}
  def __init__(self, params):
    Storage.__init__(self, params)
    self.container_size_ = 512 * 1<<10
  def configure(self, params, new_block_handler):
    Storage.configure(self, params, new_block_handler)
  def load_configuration(self, new_block_handler):
    Storage.load_configuration(self, new_block_handler)
  def get_cur_files(self):
    if not self.files.has_key(self.config['key']):
      self.files[self.config['key']] = {}
    return self.files[self.config['key']]
  def set_container_size(self, container_size):
    self.container_size_ = container_size
  def container_size(self):
    return self.container_size_
  def list_container_files(self):
    return self.get_cur_files().keys()
  def open_header_file(self, sequence_id, index):
    return StringIO.StringIO()
  def open_body_file(self, sequence_id, index):
    return StringIO.StringIO()
  def upload_container(self, sequence_id, index, header_file, body_file):
    assert sequence_id == self.active_sequence_id
    file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)
    self.get_cur_files()[file_name] = (header_file.getvalue() +
      body_file.getvalue())
  def load_body_file(self, sequence_id, index):
    body_file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)
    return StringIO.StringIO(self.get_cur_files()[body_file_name])

class FTPStorage(Storage):
  """
  Handler for a FTP site storage
  """
  def __init__(self, params, RemoteHandlerClass):
    Storage.__init__(self, params)
    self.RemoteHandlerClass = RemoteHandlerClass
    self.fs_handler = None
    #self.up_bw_limiter = BandwidthLimiter(15.0E3)
    #self.down_bw_limiter = BandwidthLimiter(10000.0E3)
  def configure(self, params, new_block_handler):
    Storage.configure(self, params, new_block_handler)
  def load_configuration(self, new_block_handler):
    Storage.load_configuration(self, new_block_handler)
    #print "Loaded directory storage configuration", self.config

  def get_fs_handler(self):
    if self.fs_handler is None:
      self.fs_handler = self.RemoteHandlerClass(self.get_host(),
        self.get_user(), self.get_password(), self.get_pkey_file(),
        self.get_path())
    self.fs_handler.set_progress_reporter(self.report_manager.find_reporter(
      "container.progress", 0))
    return self.fs_handler

  def get_host(self):
    return self.config["host"]
  
  def get_user(self):
    return self.config["user"]
  
  def get_password(self):
    if not self.config.has_key("password"):
      return None
    return self.config["password"]
  
  def get_pkey_file(self):
    if not self.config.has_key("pkey_file"):
      return None
    return self.config["pkey_file"]

  def get_path(self):
    return self.config["path"]

  def container_size(self):
    return 16<<20
  
  def list_container_files(self):
    logging.info("Scanning containers:")
    file_list = self.get_fs_handler().list_files()
    logging.info("listed files " + str(sorted(file_list)))
    return file_list

  def open_header_file(self, sequence_id, index):
    logging.debug("Starting container header %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    return StringIO.StringIO()
  def open_body_file(self, sequence_id, index):
    logging.debug("Starting container body %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    return StringIO.StringIO()
  
  def load_body_file(self, sequence_id, index):
    logging.debug("Loading container body %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)
    filehandle = tempfile.TemporaryFile(dir=Config.paths.staging_area())
    self.get_fs_handler().download(filehandle, file_name)
    filehandle.seek(0)
    return filehandle
  
  def upload_container(self, sequence_id, index, header_file, body_file):
    logging.info("Uploading container %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    assert sequence_id == self.active_sequence_id
    
    start_time = time.time()

    # Copy header_file + body_file > tmpfile
    tmpfile = tempfile.TemporaryFile(dir=Config.paths.staging_area())
    total_size = 0
    header_file.seek(0)
    while True:
      block = header_file.read(64 << 10)
      if len(block) == 0: break
      tmpfile.write(block)
      total_size += len(block)
    body_file.seek(0)
    while True:
      block = body_file.read(64 << 10)
      if len(block) == 0: break
      tmpfile.write(block)
      total_size += len(block)

    self.report_manager.set(
        "storage.container.upload.%s.%d.size" % (
          base64.b64encode(sequence_id), index),
        "%d" % total_size)

    # Upload tmpfile to the remote location
    tmp_file_name = encode_container_name(sequence_id, index, CONTAINER_EXT_TMP)
    file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)

    tmpfile.seek(0)
    self.get_fs_handler().upload(tmpfile, tmp_file_name)
    self.get_fs_handler().rename(tmp_file_name, file_name)
    self.get_fs_handler().chmod(file_name, 0440)

    self.report_manager.set(
        "storage.container.upload.%s.%d.time" % (
          base64.b64encode(sequence_id), index),
        "%f" % (time.time() - start_time))

class DirectoryStorage(Storage):
  """
  Handler for a simple directory.
  """
  def __init__(self, params):
    Storage.__init__(self, params)
  def configure(self, params, new_block_handler):
    #print "Configuring directory storage with parameters", params
    Storage.configure(self, params, new_block_handler)
  def load_configuration(self, new_block_handler):
    Storage.load_configuration(self, new_block_handler)
    #print "Loaded directory storage configuration", self.config
  def get_path(self):
    return self.config["path"]
  def container_size(self):
    #return 4<<20
    return 16<<20
  def list_container_files(self):
    return os.listdir(self.get_path())
  def open_header_file(self, sequence_id, index):
    logging.debug("Starting container header %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    return StringIO.StringIO()
  def open_body_file(self, sequence_id, index):
    logging.debug("Starting container body %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    return StringIO.StringIO()
  def upload_container(self, sequence_id, index, header_file, body_file):
    # Write the header file to summary header
    assert sequence_id == self.active_sequence_id
    
    logging.debug("Uploading container %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    file_name_tmp = encode_container_name(sequence_id, index, CONTAINER_EXT_TMP)
    file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)
    file_path_tmp = os.path.join(self.get_path(), file_name_tmp)
    file_path = os.path.join(self.get_path(), file_name)

    if os.path.isfile(file_path_tmp):
      # If the tmp file exists, it has been there earlier, but we can't trust
      # its contents (It may be half-uploaded).
      os.unlink(file_path_tmp)
    file_stream = open(file_path_tmp, "wb")
    header_file.seek(0)
    while True:
      block = header_file.read(64 << 10)
      if len(block) == 0: break
      file_stream.write(block)
    body_file.seek(0)
    while True:
      block = body_file.read(64 << 10)
      if len(block) == 0: break
      file_stream.write(block)
    file_stream.close()
    # Rename the tmp file to permanent one
    shutil.move(file_path_tmp, file_path)
    # Remove the write permission off the permanent files
    os.chmod(file_path, 0444)
  def load_body_file(self, sequence_id, index):
    logging.debug("Loading container %s %d" %
      (base64.urlsafe_b64encode(sequence_id), index))
    file_name = encode_container_name(sequence_id, index, CONTAINER_EXT)
    file_path = os.path.join(self.get_path(), file_name)
    return open(file_path, "rb")

import smtplib
import email.Encoders as Encoders
import email.Message as Message
import email.MIMEAudio as MIMEAudio
import email.MIMEBase as MIMEBase
import email.MIMEMultipart as MIMEMultipart
import email.MIMEImage as MIMEImage
import email.MIMEText as MIMEText

class MailStorage(Storage):
  """
  Handler for gmail container.
  Needs a list of gmail addresses.
  """
  def __init__(self):
    Storage.__init__(self)
    self.accounts = []
  def init(self, params):
    Storage.init(self, params)
    self.backup = backup
    self.username = params[0]
    self.password = params[1]
    self.quota = int(params[2])
  def configure(self, username, password, quota):
    self.add_account(username, password, quota)
  def load(self,filename):
    file = open(filename, "rb")
    Storage.load(self,file)
    configLine = file.readline()
    (username,password,quota) = re.split("\s+",configLine)[0:3]
    self.add_account(username,password,quota)
  def save(self,filename):
    file = open(filename,"wb")
    Storage.save(self,file)
    account = self.accounts[0]
    file.write("%s %s %s" %
      (account["user"],account["pass"],account["quota"]))
  def add_account(self,username,password,quota):
    self.accounts.append(
      {"user":username, "pass":password, "quota":quota, "used":0})
  def container_size(self):
    return 2<<20
  def save_container(self, container):
    print "Saving container"

    s = smtplib.SMTP()
    s.set_debuglevel(1)
    print "connecting"
    #s.connect("gmail-smtp-in.l.google.com")
    s.connect("alt2.gmail-smtp-in.l.google.com")
    #s.connect("smtp.gmail.com", 587)
    print "starting tls"
    s.ehlo("www.manent.net")
    s.starttls()
    s.ehlo("www.manent.net")
    print "logging in"
    
    print "sending header in mail"
    #s.set_debuglevel(0)
    header_msg = MIMEMultipart()
    header_msg["Subject"] = "manent.%s.%s.header" % (
      self.backup.label, str(container.index))
    #header_msg["To"] = "gsasha@gmail.com"
    #header_msg["From"] = "gsasha@gmail.com"
    header_attch = MIMEBase("application", "manent-container")
    filename = container.filename()
    header_file = open(os.path.join(
      Config.paths.staging_area(), filename), "rb")
    header_attch.set_payload(header_file.read())
    header_file.close()
    Encoders.encode_base64(header_attch)
    header_msg.attach(header_attch)
    s.set_debuglevel(0)
    s.sendmail("gsasha.manent1@gmail.com",
      "gsasha.manent1@gmail.com", header_msg.as_string())
    s.set_debuglevel(1)
    
    print "sending data in mail"
    data_msg = MIMEMultipart()
    data_msg["Subject"] = "manent.%s.%s.data" % (self.backup.label,
      str(container.index))
    data_attch = MIMEBase("application", "manent-container")
    filename = container.filename()
    data_file = open(
      os.path.join(Config.paths.staging_area(), filename+".data"), "rb")
    data_attch.set_payload(data_file.read())
    data_file.close()
    Encoders.encode_base64(data_attch)
    data_msg.attach(data_attch)
    s.set_debuglevel(0)
    s.sendmail("gsasha.manent1@gmail.com",
      "gsasha.manent1@gmail.com", data_msg.as_string())
    s.set_debuglevel(1)
    print "all done"
    
    s.close()
    print header_msg.as_string()

class OpticalStorage(Storage):
  """
  Handler for optical container.
  Can be one of: CD-650, CD-700, DVD, DVD-DL, BLURAY :)
  """
  CONTAINER_TYPES = {
    "CD-650" : 650<<20,
    "CD-700" : 700<<20,
    "DVD"    : 4600<<20,
    "DVD-DL" : 8500<<20,
    "BLURAY" : 26000<<20,
    }
  def __init__(self, params):
    self.containerType = NONE
    self.containers = []
    raise "not implemented"
  def configure(self, params):
    (containerType,) = params
    self.containerType = containerType
    if not CONTAINER_TYPES.has_key(self.containerType):
      print "Unknown container type", self.containerType
      exit(1)
  def load(self,filename):
    file = open(filename, "rb")
    Storage.load(self,file)
    containerType = file.readline()
    for line in file:
      self.containers.append(line)
  def save(self,file):
    file = open(filename,"wb")
    Storage.save(self,file)
    file.write(self.containerType+"\n")
    for container in self.containers:
      file.write(container+"\n")
  def container_size(self):
    return CONTAINER_TYPES[self.containerType]

# Utility functions
def encode_container_name(sequence_id, index, extension):
  return "%s.%s.%s" % (base64.urlsafe_b64encode(sequence_id),
      IE.ascii_encode_int_varlen(index), extension)

def decode_container_name(name):
  name_re = re.compile("([^.]+).([^.]+).([^.]+)", re.UNICODE)
  match = name_re.match(name)
  if not match:
    return (None, None, None)
  try:
    sequence_id = base64.urlsafe_b64decode(match.groups()[0].encode('utf8'))
    index = IE.ascii_decode_int_varlen(match.groups()[1].encode('utf8'))
    extension = match.groups()[2]
    return (sequence_id, index, extension)
  except:
    # File name unparseable. Can be junk coming from something else
    traceback.print_exc()
    return (None, None, None)
