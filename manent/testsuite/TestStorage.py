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
	def test_sequence_created(self):
		"""Test that unique sequence ids are created"""
		env = Database.MockDatabaseConfig()
		config_db = env.get_database_btree("a", None)
		storage = Storage.DirectoryStorage(0, config_db)
		storage.make_active()
		CONFIGURATION = {"path": "/tmp"}
		seq_id1 = storage.create_sequence()
		container1 = storage.create_container()
		seq_id2 = storage.create_sequence()
		self.failUnless(seq_id1 != seq_id2)
		container2 = storage.create_container()
	def test_container_created(self):
		"""Test that containers are created and restored correctly"""
		pass
	def test_sequence_restored(self):
		"""Test that once a sequence is created, the next instantiation of storage with
		the same sequence sees it"""
		pass
	def test_active_sequence_restored(self):
		"""Test that once an active sequence is created, the same sequence is reported
		as active in the new invocation of the same storage"""
		pass
	def test_new_containers_visible(self):
		"""Test that the new containers appearing in all the sequences are visible"""
		pass
	def test_new_containers_in_active_sequence_caught(self):
		"""Test that if new containers appear unexpectedly in the active sequence,
		it is actually discovered"""
		pass
	def test_new_active_sequence(self):
		"""Test that when the storage is recreated from a new db, the existing active
		sequence is not restored"""
		pass
