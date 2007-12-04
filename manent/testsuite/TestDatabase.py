import unittest
import random
import os,shutil
import exceptions
import traceback

import manent.Database as DB

TEST_DIR = "/tmp/test-manent-db"

class MockupConfig:
	def __init__(self):
		pass
	def home_area(self):
		return TEST_DIR
	def staging_area(self):
		return TEST_DIR

class TestDatabase(unittest.TestCase):
	def setUp(self):
		os.mkdir(TEST_DIR)
		self.config = MockupConfig()
	def tearDown(self):
		shutil.rmtree(TEST_DIR)

	def testAAIterate(self):
		try:
			dbc = DB.DatabaseConfig(self.config,"test1")
			txn = DB.TransactionHandler(dbc)
			db = dbc.get_database_btree("table3",txn)
			db["aaa1"] = "bbb1"
			db["aaa2"] = "bbb2"
			db["aaa3"] = "bbb3"
			db_vals = [(key, val) for (key, val) in db]
			self.assertEqual(db_vals, [("aaa1", "bbb1"), ("aaa2", "bbb2")])
			txn.commit()
		except:
			print "Caught exception"
			traceback.print_exc()
			txn.abort()
		finally:
			db.close()
			dbc.close()

	def testCommit(self):
		try:
			dbc = DB.DatabaseConfig(self.config,"test1")
			txn = DB.TransactionHandler(dbc)
			db = dbc.get_database("table1",txn)
			db["kuku"] = "bebe"
			txn.commit()
			self.assertEqual(db["kuku"],"bebe")
		finally:
			txn.commit()
			db.close()
			dbc.close()

	def testAbort(self):
		try:
			dbc = DB.DatabaseConfig(self.config,"test1")
			txn = DB.TransactionHandler(dbc)
			db = dbc.get_database("table2",txn)
			db["kuku"] = "bebe"
			txn.abort()
			self.assertRaises(exceptions.Exception,db.get,"kuku")
			#db.close()
			db = dbc.get_database("table2",txn)
			self.assertEqual(db["kuku"],None)
		finally:
			txn.commit()
			db.close()
			dbc.close()

