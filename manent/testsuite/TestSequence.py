#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import shutil
import sys
import cStringIO as StringIO
import tempfile
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Container as Container
import manent.Database as Database
import manent.Sequence as Sequence
import manent.Storage as Storage
import manent.utils.Digest as Digest

class MockStorage:
  def __init__(self, config_db):
    self.config_db = config_db
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.summary_headers_db = self.env.get_database_btree("summary_headers_db",
        None, None)
  def container_size(self):
    return 1024
  def _key(self, suffix):
    return "sequence_key." + suffix

class TestSequence(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.config_db = self.env.get_database_btree("TestSequenceConfigDB",
      None, None)
    self.config_db.truncate()
    self.storage = MockStorage(self.config_db)
    #print "tmp path=", self.scratch_path
  def tearDown(self):
    pass
  def test_get_num_piggyback_headers(self):
    EXPECTED = [(0, 0), (1, 0), (2, 0), (3, 0), (4, 4), (5, 0), (6, 0), (7, 0),
        (8, 4), (9, 0), (10, 0), (11, 0), (12, 4),
        (16, 16), (32, 16), (64, 64)]
    sequence = Sequence.Sequence(self.storage, "sequence_a", False)
    for input, output in EXPECTED:
      result = sequence.get_num_piggyback_headers(input)
      logging.debug("Testing get_num_piggyback_headers(%d)->%d"
          % (input, result))
      self.assertEquals(output, result)
  def test_get_next_index(self):
    sequence = Sequence.Sequence(self.storage, "sequence_a", False)
    self.assertEquals(0, sequence.get_next_index())
    self.assertEquals(1, sequence.get_next_index())
    self.assertEquals(2, sequence.get_next_index())
    # Recreate the sequence, see that sequence numbers continue
    sequence = Sequence.Sequence(self.storage, "sequence_a", False)
    self.assertEquals(3, sequence.get_next_index())
  def test_add_sequences(self):
    sequence = Sequence.Sequence(self.storage, "sequence_a", False)
    sequence.register_new_file(0)
    sequence.process_new_files()
  def test_summary_containers_load_one(self):
    # Test that we can create one summary container for headers 0, 1, 2, 3
    # and after we reload it, the headers are read from summary
    sequence = Sequence.Sequence(self.storage, "sequence_a", False)
    for i in range(4):
      sequence.add_summary_header(i, StringIO.StringIO("Summary%d" % i))
    # TODO: reload and check
    self.fail()
  def test_summary_containers_load_two(self):
    # Test that we can create two summary containers:
    # - one for headers 0, 1
    # - one for headers 2, 3
    # and after we load the sequence, it reads both summary containers
    self.fail()
  def test_summary_container_load_partial(self):
    # Test that we can create one summary container for headers 0, 1, 2
    # and there is another header 3, then after we load the sequence,
    # the available headers are loaded from summary
    self.fail()
  def test_summary_container_overlap(self):
    # Test that we can create 3 summary container:
    # one for headers 0, 1, 2
    # one for headers 1, 2, 3
    # one for headers 3, 4, 5
    # and after we reload the sequence, summary 2 and 5 are loaded
    self.fail()
  def test_write_summary_container(self):
    # Test that summary container is written when the container gets full
    self.fail()
  def test_write_summary_after_reload(self):
    # Test that we can create summary container for headers 0, 1,
    # and create headers 2, 3
    # Then, we reload the sequence and create new headers 4, 5
    # Then, a summary container is created and has headers 2, 3, 4, 5
    self.fail()
  def test_new_containers_reloaded(self):
    # Test that after we reload a sequence, all new containers are reported
    self.fail()
