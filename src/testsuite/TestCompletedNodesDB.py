#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import sys
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.CompletedNodesDB as CNDB
import manent.Config as Config
import manent.Database as Database
import manent.Reporting as Reporting

class TestCompletedNodesDB(unittest.TestCase):
  def setUp(self):
    self.env = Database.PrivateDatabaseManager()
    self.txn = Database.TransactionHandler(self.env)
    self.report_manager = Reporting.ReportManager()
    self.env.set_report_manager(self.report_manager)
  def tearDown(self):
    self.txn.abort()
    self.env.close()
    self.env = None
    Config.paths.clean_temp_area()
  def test_add_block(self):
    # Test that when block is added, it is visible
    cndb = CNDB.CompletedNodesDB(self.env, self.txn)
    self.failIf(cndb.has_key("k1"))
    cndb["k1"] = "v1"
    self.assert_(cndb.has_key("k1"))
    cndb.close()
  def test_remove_block(self):
    # Test that after a block is removed, it is no longer visible
    cndb = CNDB.CompletedNodesDB(self.env, self.txn)
    self.failIf(cndb.has_key("k1"))
    cndb["k1"] = "v1"
    del cndb["k1"]
    cndb.close()
  def test_count_db_ops(self):
    # Use the counters built in to the DB to make sure that it is touched only
    # on load and save, and that it is touched only as many times as necessary.
    cndb = CNDB.CompletedNodesDB(self.env, self.txn)
    accesses_before = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    for i in range(100):
      cndb["k%d" % i] = "v%d" %i
    for i in range(90):
      del cndb["k%d" % i]
    accesses_after = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    self.assertEqual(accesses_before, accesses_after)
    cndb.save()
    accesses_after_save = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    self.assertNotEqual(accesses_after, accesses_after_save)
    cndb.load()
    accesses_after_load = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    self.assertNotEqual(accesses_after_load, accesses_after_save)
    cndb.close()
  def test_reload(self):
    # Test that a reload works.
    cndb1 = CNDB.CompletedNodesDB(self.env, self.txn)
    cndb1["k1"] = "v1"
    cndb1.save()
    cndb2 = CNDB.CompletedNodesDB(self.env, self.txn)
    cndb2.load()
    self.assert_(cndb2.has_key("k1"))
    self.assertEqual("v1", cndb2["k1"])
    cndb1.close()
    cndb2.close()
  def test_empty_reload(self):
    # Test that a reload of emptied database is set.
    cndb1 = CNDB.CompletedNodesDB(self.env, self.txn)
    cndb1["k1"] = "v1"
    cndb1.save()
    del cndb1["k1"]
    cndb1.save()
    cndb2 = CNDB.CompletedNodesDB(self.env, self.txn)
    cndb2.load()
    self.failIf(cndb2.has_key("k1"))
    cndb1.close()
    cndb2.close()
  def test_precommit_hook(self):
    # Test that CNDB's save is triggered on presubmit hook
    cndb = CNDB.CompletedNodesDB(self.env, self.txn)
    accesses_before = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    for i in range(100):
      cndb["k%d" % i] = "v%d" %i
    self.txn.commit()
    accesses_after = (self.env.num_puts_reporter.value +
        self.env.num_gets_reporter.value +
        self.env.num_dels_reporter.value +
        self.env.num_has_keys_reporter.value)
    self.assertNotEqual(accesses_before, accesses_after)
    cndb.close()
