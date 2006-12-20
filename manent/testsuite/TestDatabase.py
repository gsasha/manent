import unittest
import random
import os,shutil
import exceptions

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
