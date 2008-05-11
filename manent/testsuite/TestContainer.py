#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import cStringIO as StringIO
import logging
import os
import random
import sys
import tempfile
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.utils.Digest as Digest

#random.seed(23423)

class MockHandler:
  def __init__(self):
    self.expected = {}
    self.data_loaded = {}
  def add_expected(self, digest, code, data):
    self.expected[digest] = (code, data)
  def is_requested(self, digest, code):
    if code >= Container.CODE_CONTROL_START:
      return False
    if self.expected.has_key(digest):
      return True
    if code != Container.CODE_DATA:
      return True
    return False
  def loaded(self, digest, code, data):
    self.data_loaded[digest] = (code, data)
  def check(self):
    for k in self.data_loaded.keys():
      if not self.expected.has_key(k):
        print "block %s not expected" % base64.b64encode(k)
        return False
      assert self.expected.has_key(k)
      assert self.data_loaded.has_key(k)
      expected = self.expected[k][0]
      loaded = self.data_loaded[k][0]
      if expected != loaded:
        print "block %s has loaded different data" % base64.b64encode(k)
        print " Expected %d:"%len(expected), base64.b64encode(expected)
        print " Loaded   %d:"%len(loaded), base64.b64encode(loaded)
        return False
    for k in self.expected.keys():
      if not self.data_loaded.has_key(k):
        print "block %s:%d not loaded" % (base64.b64encode(k),
          self.expected[k][1])
        return False
    return True

class MockStorage:
  def __init__(self, password):
    self.password = password
    self.containers = {}
    self.container_sizes = {}
    self.tempdir = tempfile.mkdtemp()

    self.cur_index = 0

  def cleanup(self):
    # Nothing to do on cleanup: everything is stored in memory.
    pass

  def get_container(self, index):
    # We can load header file or body file only after it was written, i.e.,
    # after open_header_file() and upload() were done.
    container = Container.Container(self)
    container.start_load("sequence_a", index)
    return container

  def get_encryption_key(self):
    return self.password

  def load_header_file(self, sequence_id, index):
    header, body, container = self.containers[index]
    container.seek(0)
    logging.debug("Container %d file has size %d" %
        (index, len(container.getvalue())))
    return container
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
    logging.debug("Container body %s" % base64.b16encode(body_contents))
    # header and body should not be used anymore after container was uploaded.
    self.containers[index] = (None, None, container)
    self.container_sizes[index] = (len(header_contents),
        len(body_contents))

  def container_size(self):
    return Container.MAX_COMPRESSED_DATA + 1024

  def _open_container_files(self, index):
    if self.containers.has_key(index):
      raise Exception("Container %d already created" % index)
    header_file = StringIO.StringIO()
    body_file = StringIO.StringIO()
    container_file = StringIO.StringIO()
    self.containers[index] = (header_file, body_file, container_file)

DATA = [
  "",
  "asdf;kasdfj;dlfksdjf;lfjdsfl;dsfdjsfsdf",
  "affdfdfdffasffdffdf",
  "manent is a backup program that supports",
  "encryption, compression and bandwitdh conservation"
  "manent is a backup program that supports",
  "encryption, compression and bandwitdh conservation"
  "manent is a backup program that supports",
  "encryption, compression and bandwitdh conservation"
  ]

class TestContainer(unittest.TestCase):

  def setUp(self):
    #self.env = Database.PrivateDatabaseManager()
    #self.txn = Database.TransactionHandler(self.env)
    #self.storage = Storage.create_storage(self.env, self.txn, 0,
    #    {'type': "__mock__", 'key': '1'}, MockHandler())
    self.storage = MockStorage("kakamaika")
  def tearDown(self):
    # Clean up the state, to make sure tests don't
    # interfere.
    Storage.MemoryStorage.files = {}
  def test_data_dumper(self):
    # Basic test of data dumper: data in, data out
    handler = MockHandler()
    outfile = StringIO.StringIO()
    dumper = Container.DataDumper(outfile)

    for d in DATA:
      digest = Digest.dataDigest(d)
      dumper.add_block(digest, Container.CODE_DATA, d)
      handler.add_expected(digest, Container.CODE_DATA, d)

    infile = StringIO.StringIO(outfile.getvalue())
    blocks = dumper.get_blocks()
    
    undumper = Container.DataDumpLoader(infile, blocks, password=None)
    undumper.load_blocks(handler)

    self.failUnless(handler.check())
  
  def test_data_dumper_compress(self):
    # Test data dumper when compression is enabled
    handler = MockHandler()
    outfile = StringIO.StringIO()
    dumper = Container.DataDumper(outfile)
    
    dumper.start_compression(Container.CODE_COMPRESSION_BZ2)
    for d in DATA:
      digest = Digest.dataDigest(d)
      dumper.add_block(digest, Container.CODE_DATA, d)
      handler.add_expected(digest, Container.CODE_DATA, d)
    dumper.stop_compression()

    infile = StringIO.StringIO(outfile.getvalue())
    blocks = dumper.get_blocks()
    
    undumper = Container.DataDumpLoader(infile, blocks, password=None)
    undumper.load_blocks(handler)

    self.failUnless(handler.check())

  def test_data_dumper_encrypt(self):
    # Test data dumper when encryption is enabled
    handler = MockHandler()
    outfile = StringIO.StringIO()
    dumper = Container.DataDumper(outfile)

    seed = Digest.dataDigest("1")
    dumper.start_encryption(Container.CODE_ENCRYPTION_ARC4, seed,
      "kakamaika")
    for d in DATA:
      digest = Digest.dataDigest(d)
      dumper.add_block(digest, Container.CODE_DATA, d)
      handler.add_expected(digest, Container.CODE_DATA, d)
    dumper.stop_encryption()

    infile = StringIO.StringIO(outfile.getvalue())
    blocks = dumper.get_blocks()

    undumper = Container.DataDumpLoader(infile, blocks,
      password="kakamaika")
    undumper.load_blocks(handler)

    self.failUnless(handler.check())

  def test_data_dumper_stress(self):
    # Test with really lots of randomly generated data
    handler = MockHandler()
    outfile = StringIO.StringIO()
    dumper = Container.DataDumper(outfile)

    encryption_active = None
    compression_active = None

    known_blocks = {}

    for i in range(10000):
      action = random.randint(0,2)
      
      if compression_active is not None:
        compression_active -= 1
        if compression_active == 0:
          dumper.stop_compression()
          compression_active = None
        else:
          #print "  Compression has %d rounds to go"%compression_active
          pass

      if encryption_active is not None:
        encryption_active -= 1
        if encryption_active == 0:
          if compression_active is not None:
            # If we need to stop encryption, compression must be stopped first
            dumper.stop_compression()
          #print "  Stopping encryption"
          dumper.stop_encryption()
          encryption_active = None
        else:
          #print "  Encryption has %d rounds to go"%encryption_active
          pass

      if action==0:
        # Generate new data item
        data_size = random.randint(0,1000)
        data = os.urandom(data_size)
        code = random.choice([
          Container.CODE_DATA,
          Container.CODE_DIR,
          Container.CODE_DATA,
          Container.CODE_DATA_PACKER,
          Container.CODE_DATA,
          Container.CODE_DIR_PACKER,
          Container.CODE_DATA,
          Container.CODE_INCREMENT_DESCRIPTOR])
        digest = Digest.dataDigest(data)

        if code == Container.CODE_DATA and known_blocks.has_key(digest):
          # We can't expect the same data block to be added twice to a container
          continue
        known_blocks[digest] = 1
        
        dumper.add_block(digest, code, data)

        # test not requesting to reload every CODE_DATA item
        if code != Container.CODE_DATA or random.randint(0, 100) > 90:
          handler.add_expected(digest, code, data)

      elif action==1:
        #continue
        # Try to start encryption
        # We can start encryption only if it is not active already
        if encryption_active != None:
          continue
        if compression_active != None:
          continue
        encryption_active = random.randint(1,100)
        #print "  Starting encryption for %d rounds"
        seed = os.urandom(Digest.dataDigestSize())
        dumper.start_encryption(Container.CODE_ENCRYPTION_ARC4, seed,
          "kakamaika")
        
      elif action==2:
        # Try to start compression
        if compression_active != None:
          continue
        compression_active = random.randint(1,100)
        if encryption_active != None:
          compression_active = min(compression_active, encryption_active)
        algorithm = random.choice([Container.CODE_COMPRESSION_BZ2])
        dumper.start_compression(algorithm)

    if compression_active is not None:
      dumper.stop_compression()
    if encryption_active is not None:
      dumper.stop_encryption()

    infile = StringIO.StringIO(outfile.getvalue())
    blocks = dumper.get_blocks()
    #print "blocks:"
    #for digest,size,code in blocks:
      #print base64.b64encode(digest), size, code

    undumper = Container.DataDumpLoader(infile, blocks,
      password="kakamaika")
    undumper.load_blocks(handler)

    self.failUnless(handler.check())

  def test_container(self):
    # Test that container is created correctly.
    # See that the container is created, stored and reloaded back,
    # and that all blocks get restored
    handler = MockHandler()
    container = self.storage.create_container()
    for d in DATA:
      container.add_block(Digest.dataDigest(d), Container.CODE_DATA, d)
      handler.add_expected(Digest.dataDigest(d), Container.CODE_DATA, d)
    self.storage.finalize_container(container)
    index = container.index

    container = self.storage.get_container(index)
    container.load_blocks(handler)
    self.failUnless(handler.check())
    
  def test_piggyback_headers_small_headers(self):
    # Test that if we are piggybacking small headers, all of them will be
    # inserted into the container.
    storage = MockStorage(password="kakamaika")
    handler = MockHandler()

    # Test that:
    # 1. Container 
    EXPECTED_HEADER_COUNTS = {
        0: 0,
        4: 4,
        8: 4,
        15: 0,
        16: 16,
        17: 0,
        64: 64}
    header_contents = "header"
    for index in range(120):
      container = storage.create_container()
      if EXPECTED_HEADER_COUNTS.has_key(container.get_index()):
        exp_headers = EXPECTED_HEADER_COUNTS[container.get_index()]
        for i in range(exp_headers):
          logging.debug("Adding piggyback header %d to container %d" %
              (i, container.get_index()))
          self.assert_(container.can_add_piggyback_header(header_contents))
          container.add_piggyback_header(exp_headers - i, header_contents)
        self.failIf(container.can_add_piggyback_header(header_contents))
      storage.finalize_container(container)
      storage.cleanup()
      # TODO(gsasha): Read the container to see that we get the headers back.
      self.fail()
  def test_piggyback_headers_large_headers(self):
    # Test that if we are trying to piggyback large headers, they would be
    # refused by the container.
    self.fail()
