#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import cStringIO as StringIO
import logging
import os
import sys
import tempfile

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Container as Container
import manent.Nodes as Nodes
import manent.Reporting as Reporting

class MockContainerConfig:
  def blockSize(self):
    return 32

class MockGlobalConfig:
  def excludes(self):
    return []

class MockIncrement:
  pass

class MockIncrementsDB:
  def __init__(self):
    self.next_index = 0
    self.increments = {}
  def start_increment(self, comment):
    self.increments[self.next_index] = comment
    increment = MockIncrement()
    increment.idx = self.next_index
    self.next_index += 1
  def finalize_increment(self):
    pass

class MockDBWrapper:
  def __init__(self):
    self.data = {}

class MockBlockCtx:
  def __init__(self, backup):
    self.backup = backup
  def add_block(self, digest, data):
    self.backup.add_block(digest, data)
  def load_block(self, digest):
    return self.backup.load_block(digest)

class MockHlinkCtx:
  def __init__(self):
    self.inodes_db = {}
  
class MockChangeCtx:
  def __init__(self):
    self.total_nodes = 0
    self.changed_nodes = 0
  def get_change_percent(self):
    if self.total_nodes == 0:
      return 0.0
    percent = float(self.changed_nodes)/self.total_nodes
    return percent
    
class MockScanCtx(MockBlockCtx, MockHlinkCtx, MockChangeCtx):
  def __init__(self,backup):
    MockBlockCtx.__init__(self, backup)
    MockHlinkCtx.__init__(self)
    MockChangeCtx.__init__(self)
    self.num_visited_files_reporter = Reporting.DummyReporter()
    self.num_visited_dirs_reporter = Reporting.DummyReporter()
    self.num_visited_symlinks_reporter = Reporting.DummyReporter()
    self.num_scanned_files_reporter = Reporting.DummyReporter()
    self.num_scanned_dirs_reporter = Reporting.DummyReporter()
    self.num_scanned_symlinks_reporter = Reporting.DummyReporter()
    self.num_prev_files_reporter = Reporting.DummyReporter()
    self.num_prev_symlinks_reporter = Reporting.DummyReporter()
    self.num_prev_dirs_reporter = Reporting.DummyReporter()
    self.num_changed_files_reporter = Reporting.DummyReporter()
    self.num_changed_symlinks_reporter = Reporting.DummyReporter()
    self.num_changed_dirs_reporter = Reporting.DummyReporter()

    self.changed_files_reporter = Reporting.DummyReporter()
    self.changed_dirs_reporter = Reporting.DummyReporter()
    self.changed_symlinks_reporter = Reporting.DummyReporter()

    self.num_new_blocks_reporter = Reporting.DummyReporter()
    self.size_new_blocks_reporter = Reporting.DummyReporter()

    self.unrecognized_files_reporter = Reporting.DummyReporter()
    self.oserror_files_reporter = Reporting.DummyReporter()
    self.ioerror_files_reporter = Reporting.DummyReporter()

    self.current_scanned_file_reporter = Reporting.DummyReporter()
  def get_level(self):
    # Assume that backup filled that in
    return self.level
  def update_scan_status(self):
    pass

class MockRestoreCtx(MockBlockCtx, MockHlinkCtx):
  def __init__(self,backup):
    MockBlockCtx.__init__(self, backup)
    MockHlinkCtx.__init__(self)

class MockRepository:
  def __init__(self):
    self.blocks_db = {}
    self.blocks_codes_db = {}
  def load_block(self, digest):
    return self.blocks_db[digest]
  def add_block(self, digest, code, data):
    self.blocks_db[digest] = data
    self.blocks_codes_db[digest] = code
  def block_code(self, digest):
    return self.blocks_codes_db[digest]

class MockBlockDatabase:
  def __init__(self,repository):
    self.repository = repository
  def request_block(self, digest):
    pass
  def add_block(self, digest, code, data):
    self.repository.add_block(digest, code, data)
  def load_block(self, digest):
    return self.repository.load_block(digest)
  def get_block_storage(self, digest):
    pass
  def get_storage_index(self, digest):
    return 0
  def get_block_type(self, digest):
    pass
  def get_active_storage_index(self):
    return 0

class MockBackup:
  def __init__(self, home):
    self.container_config = MockContainerConfig()
    self.global_config = MockGlobalConfig()
    self.increments = MockIncrementsDB()
    self.repository = MockRepository()
    self.config_db = {}
    self.completed_nodes_db = {}
    self.home = home
  def get_block_size(self):
    return 1024
  
  def start_increment(self, comment):
    increment = self.increments.start_increment(comment)
    ctx = MockScanCtx(self)

    self.ctx = ctx
    self.root_node = Nodes.Directory(self, None, self.home)
    return ctx
  def finalize_increment(self):
    self.increments.finalize_increment()
  def start_restore(self,idx):
    ctx = MockRestoreCtx(self)
    return ctx
  def is_increment_finalized(self,idx):
    return self.increments.is_increment_finalized(idx)
  
  def add_block(self, digest, code, data):
    self.repository.add_block(digest, code, data)
  def load_block(self, digest):
    return self.repository.load_block(digest)
  def get_block_code(self, digest):
    return self.repository.block_code(digest)

  def get_completed_nodes_db(self):
    return self.completed_nodes_db

class MockStorage:
  def __init__(self, password):
    self.password = password
    self.containers = {}
    self.container_sizes = {}
    self.piggybacking_headers = True
    self.tempdir = tempfile.mkdtemp()
    self.max_container_size = Container.MAX_COMPRESSED_DATA + 1024 * 1024

    self.cur_index = 0

  def set_piggybacking_headers(self, h):
    self.piggybacking_headers = h

  def get_container(self, index):
    # We can load header file or body file only after it was written, i.e.,
    # after open_header_file() and upload() were done.
    container = Container.Container(self)
    container.start_load("sequence_a", index)
    return container

  def get_encryption_key(self):
    return self.password

  def load_header_file(self, sequence_id, index):
    if self.piggybacking_headers:
      header, body, container = self.containers[index]
      container.seek(0)
      logging.debug("Container %d file has size %d" %
          (index, len(container.getvalue())))
      return container
    else:
      return None
  def load_body_file(self, sequence_id, index):
    header, body, container = self.containers[index]
    container.seek(0)
    logging.debug("Container %d file has size %d" %
        (index, len(container.getvalue())))
    hs, bs = self.container_sizes[index]
    logging.debug("Container %d file has header %s" %
        (index, base64.b16encode(container.getvalue()[:hs])))
    logging.debug("Container %d file has body %s" %
        (index, base64.b16encode(container.getvalue()[hs:])))

    assert hs + bs == len(container.getvalue())
    return container
  def get_label(self):
    return "mukakaka"
  
  def create_container(self):
    self._open_container_files(self.cur_index)
    container = Container.Container(self)
    container.start_dump("sequence_a", self.cur_index)
    self.cur_index += 1
    return container

  def open_header_file(self, sequence_id, index):
    header, body, container = self.containers[index]
    return header
  def open_body_file(self, sequence_id, index):
    header, body, container = self.containers[index]
    return body

  def finalize_container(self, container):
    container.finish_dump()
    container.upload()

  def upload_container(self, sequence_id, index, header_file, body_file):
    # To upload the container, we first write its header and then its body.
    header, body, container = self.containers[index]
    header.seek(0)
    header_contents = header.read()
    container.write(header_contents)
    body.seek(0)
    body_contents = body.read()
    container.write(body_contents)
    logging.debug("Uploaded container %d: header size=%d, body size=%d"
        % (index, len(header_contents), len(body_contents)))
    logging.debug("Container body '%s...' len= %d" %
        (base64.b16encode(body_contents)[:min(16, len(body_contents))],
          len(body_contents)))
    # header and body should not be used anymore after container was uploaded.
    self.containers[index] = (None, None, container)
    self.container_sizes[index] = (len(header_contents),
        len(body_contents))

  def container_size(self):
    return self.max_container_size

  def _open_container_files(self, index):
    if self.containers.has_key(index):
      raise Exception("Container %d already created" % index)
    header_file = StringIO.StringIO()
    body_file = StringIO.StringIO()
    container_file = StringIO.StringIO()
    self.containers[index] = (header_file, body_file, container_file)

