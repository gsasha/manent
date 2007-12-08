import os
import unittest

import manent.Database as Database
import manent.Storage as Storage

class TestStorage(unittest.TestCase):
	def test_params_stored(self):
		env = Database.MockDatabaseConfig()
		config_db = env.get_database_btree("a", None)
		storage = Storage.DirectoryStorage(0, config_db)
		CONFIGURATION = {"path": "/tmp"}
		storage.configure(CONFIGURATION)
		# Make sure that the configuration we put in is read back correctly
		self.assertEqual(storage.get_config(), CONFIGURATION)
		
		# Recreate the storage from config_db and make sure its configuration
		# returns back.
		storage = Storage.DirectoryStorage(0, config_db)
		storage.get_config()
		self.assertEqual(storage.get_config(), CONFIGURATION)
	def test_container_name(self):
		env = Database.MockDatabaseConfig()
		config_db = env.get_database_btree("a", None)
		storage = Storage.Storage(0, config_db)
		for container_idx in range(10000):
			seq_id = os.urandom(12)
			encoded = storage.encode_container_name(seq_id, container_idx, "data")
			#print "encoded", encoded
			dec_seq_id, dec_container_idx, dec_ext = storage.decode_container_name(encoded)
			self.assertEqual(seq_id, dec_seq_id)
			self.assertEqual(container_idx, dec_container_idx)
			self.assertEqual(dec_ext, "data")
	def test(self):
		env = Database.MockDatabaseConfig()
		config_db = env.get_database_btree("a", None)
		storage = Storage.DirectoryStorage(0, config_db)
		CONFIGURATION = {"path": "/tmp"}
		seq_id1 = storage.create_sequence()
		seq_id2 = storage.create_sequence()
		self.failUnless(seq_id1 != seq_id2)
