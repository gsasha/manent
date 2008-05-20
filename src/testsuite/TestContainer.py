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
import Mock

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
    self.storage = Mock.MockStorage("kakamaika")
  def tearDown(self):
    self.storage = None
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
    # and that all blocks get restored.
    self.storage.set_piggybacking_headers(False)
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
    
  def test_container_with_piggybacking(self):
    # Test that container is created correctly.
    # See that the container is created, stored and reloaded back,
    # and that all blocks get restored.
    # In this test, we ask storage to provide separate headers, as if they were
    # found through piggybacking.
    self.storage.set_piggybacking_headers(True)
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
    handler = MockHandler()

    # Test that:
    # 1. Container 
    EXPECTED_HEADER_COUNTS = {
        0: 0,
        4: 4,  # This is special case!
        8: 3,
        15: 0,
        16: 16,
        17: 0,
        32: 15,
        64: 64,
        128: 63}
    for index in range(120):
      container = self.storage.create_container()
      if EXPECTED_HEADER_COUNTS.has_key(container.get_index()):
        exp_headers = EXPECTED_HEADER_COUNTS[container.get_index()]
        for i in range(exp_headers):
          header_index = index - 1 - i
          header_contents = "header-%d" % header_index
          logging.debug("Adding piggyback header %d to container %d" %
              (header_index, container.get_index()))
          self.assert_(container.can_add_piggyback_header(header_contents))
          container.add_piggyback_header(header_index, header_contents)
        self.failIf(container.can_add_piggyback_header("HEADER_ONE_TOO_MANY"))
      # Add some data and metadata blocks to the container to check that
      # we're not confused by them.
      container.add_block(
          Digest.dataDigest("kuku"), Container.CODE_DATA, "kuku")
      container.add_block(
          Digest.dataDigest("kaka"), Container.CODE_DIR, "kaka")
      self.storage.finalize_container(container)
    class PiggybackLoadHandler:
      def __init__(self):
        self.headers = {}
      def is_requested(self, digest, code):
        return code == Container.CODE_HEADER
      def loaded(self, digest, code, data):
        assert code == Container.CODE_HEADER
        container_index = Container.decode_piggyback_container_index(digest)
        self.headers[container_index] = data
      def check(self, container_index, num_headers):
        for i in range(num_headers):
          pb_header_index = container_index - 1 - i
          assert self.headers.has_key(pb_header_index)
          expected_header = "header-%d" % pb_header_index
          received_header = self.headers[pb_header_index]
          logging.debug("Header: expected = '%s', got='%s'" %
              (expected_header, received_header))
          assert expected_header == received_header
    for index in range(120):
      if EXPECTED_HEADER_COUNTS.has_key(index):
        num_piggyback_headers = EXPECTED_HEADER_COUNTS[index]
      else:
        num_piggyback_headers = 0
      container = self.storage.get_container(index)
      handler = PiggybackLoadHandler()
      container.load_blocks(handler)
      handler.check(container_index=index, num_headers=num_piggyback_headers)
  def test_piggyback_headers_large_headers(self):
    # Test that if we are trying to piggyback large headers, they would be
    # refused by the container.
    for i in range(65):
      container = self.storage.create_container()
    self.assertEquals(64, container.get_index())
    self.assert_(container.can_add_piggyback_header("h" * 100))
    self.failIf(container.can_add_piggyback_header("h" * 1000000))
