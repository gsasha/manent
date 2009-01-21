#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import exceptions
import logging
import os
import os.path
import random
import shutil
import sys
import traceback
import unittest

# Point to the code location so that it is found when unit tests
# are executed. We assume that sys.path[0] is the path to the module
# itself. This allows the test to be executed directly by testoob.
sys.path.append(os.path.join(sys.path[0], ".."))

import manent.Config as Config
import manent.Database as DB

# The lists of names are stored as arrays of strings.
class MockPaths:
  def home_area(self):
    path = self.temp_area()
    return path

  def backup_home_area(self, label):
    path = os.path.join(self.home_area(), "BACKUP-" + label)
    return path

  def staging_area(self):
    return os.path.join(self.temp_area, "staging")

  def backup_staging_area(self, label):
    if not os.path.isdir(path):
      os.path.makedirs(path, 0700)
    return os.path.join(self.staging_area(), "BACKUP-" + label)

  def temp_area(self):
    return os.path.join(Config.paths.temp_area(), "test-manent-db")

class TestDatabase(unittest.TestCase):
  def setUp(self):
    self.path_config = MockPaths()
    if not os.path.isdir(self.path_config.backup_home_area("")):
      os.makedirs(self.path_config.backup_home_area(""))
  def tearDown(self):
    shutil.rmtree(self.path_config.backup_home_area(""))
  def testCommit(self):
    try:
      dbc = DB.DatabaseManager(self.path_config, "")
      txn = DB.TransactionHandler(dbc)
      db = dbc.get_database("table1", None, txn)
      db["kuku"] = "bebe"
      txn.commit()
      self.assertEqual(db["kuku"], "bebe")
    finally:
      txn.commit()
      db.close()
      # dbc.close()
  def testAbort(self):
    try:
      dbc = DB.DatabaseManager(self.path_config, "")
      txn = DB.TransactionHandler(dbc)
      db = dbc.get_database("table2", None, txn)
      db["kuku"] = "bebe"
      txn.abort()
      self.assertRaises(exceptions.Exception, db.get, "kuku")
      #db.close()
      db = dbc.get_database("table2", None, txn)
      self.assertEqual(db["kuku"], None)
    except:
      traceback.print_exc()
      txn.abort()
    finally:
      txn.abort()
      db.close()
      # dbc.close()
  def testIterate(self):
    try:
      dbc = DB.DatabaseManager(self.path_config, "")
      txn = DB.TransactionHandler(dbc)
      db = dbc.get_database_btree("table3", None, txn)
      vals = [("aaa1", "bbb1"), ("aaa2", "bbb2"), ("aaa3", "bbb3")]
      for key, val in vals:
        db[key] = val
      db_vals = [(key, val) for (key, val) in db.iteritems()]
      self.assertEqual(db_vals, vals)
      txn.commit()
    except:
      traceback.print_exc()
      txn.abort()
    finally:
      db.close()
      # dbc.close()
  def testIteratePrefix(self):
    try:
      dbc = DB.DatabaseManager(self.path_config, "")
      txn = DB.TransactionHandler(dbc)
      db = dbc.get_database_btree("table3", None, txn)
      vals = [("aaa1", "bbb1"), ("aab1", "bbb2"), ("aab2", "bbb3"), ("aac1", "bbb4")]
      for key, val in vals:
        db[key] = val
      db_vals = [(key, val) for (key, val) in db.iteritems_prefix("aab")]
      self.assertEqual(db_vals, [("aab1", "bbb2"), ("aab2", "bbb3")])
      txn.commit()
    except:
      traceback.print_exc()
      txn.abort()
    finally:
      db.close()
      # dbc.close()
