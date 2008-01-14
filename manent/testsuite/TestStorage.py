import os
import shutil
import tempfile
import unittest

import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.utils.Digest as Digest

class TestStorage(unittest.TestCase):
	def setUp(self):
		self.env = Database.PrivateDatabaseManager()
		self.txn = Database.TransactionHandler(self.env)
		self.config_db = self.env.get_database_btree("a", None, None)
		self.scratch_path = tempfile.mkdtemp(".storage", "manent.", "/tmp")
		self.CONFIGURATION = {"path": self.scratch_path, "password": "kuku"}
		#print "tmp path=", self.scratch_path
	def tearDown(self):
		shutil.rmtree(self.scratch_path)
	def test_params_stored(self):
		storage = Storage.DirectoryStorage(0, self.config_db)
		storage.configure(self.CONFIGURATION, None)
		# Make sure that the configuration we put in is read back correctly
		self.assertEqual(storage.get_config(), self.CONFIGURATION)
		
		# Recreate the storage from config_db and make sure its configuration
		# returns back.
		storage = Storage.DirectoryStorage(0, self.config_db)
		storage.get_config()
		self.assertEqual(storage.get_config(), self.CONFIGURATION)
	def test_container_name(self):
		storage = Storage.Storage(0, self.config_db)
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
		storage = Storage.DirectoryStorage(0, self.config_db)
		storage.configure(self.CONFIGURATION, None)
		storage.make_active()
		seq_id1 = storage.create_sequence()
		seq_id2 = storage.create_sequence()
		self.failUnless(seq_id1 != seq_id2)
	def test_active_sequence_reloaded(self):
		"""Test that the active sequence is reloaded correctly"""
		storage1 = Storage.DirectoryStorage(0, self.config_db)
		storage1.configure(self.CONFIGURATION, None)
		storage1.make_active()
		seq_id1 = storage1.get_active_sequence_id()
		
		storage2 = Storage.DirectoryStorage(0, self.config_db)
		storage2.load_configuration(None)
		seq_id2 = storage2.get_active_sequence_id()
		self.assertEqual(seq_id1, seq_id2)
	def test_sequences_reloaded(self):
		"""Test that all the sequences created get restored"""
		# Create storage and a container
		storage1 = Storage.DirectoryStorage(0, self.config_db)
		storage1.configure(self.CONFIGURATION, None)
		seq_id1 = storage1.create_sequence()
		container = storage1.create_container()
		container.finish_dump()
		container.upload()
		seq_id2 = storage1.create_sequence()
		container = storage1.create_container()
		container.finish_dump()
		container.upload()
		# Create a new db to simulate a different machine
		config_db2 = self.env.get_database_btree("b", None, None)
		storage2 = Storage.DirectoryStorage(0, config_db2)
		class NopHandler:
			def __init__(self):
				pass
			def report_new_container(self, container):
				pass
		# We know that there are new containers appearing.
		# However, we don't care in this test.
		storage2.configure(self.CONFIGURATION, NopHandler())
		sequences = storage2.get_sequence_ids()
		#print sequences
		self.assert_(seq_id1 in sequences)
		self.assert_(seq_id2 in sequences)
	def test_container_created(self):
		"""Test that containers are created and restored correctly"""
		# Create storage and a container
		storage = Storage.DirectoryStorage(0, self.config_db)
		storage.configure(self.CONFIGURATION, None)
		storage.make_active()
		seq_id = storage.get_active_sequence_id()
		container = storage.create_container()
		block = "some strange text"
		block_digest = Digest.dataDigest(block)
		container.add_block(block_digest, Container.CODE_DATA, block)
		container.finish_dump()
		container.upload()
		self.assertEqual(0, container.get_index())
		self.assertEqual(seq_id, container.get_sequence_id())

		# Reload the storage and read the container
		storage = Storage.DirectoryStorage(0, self.config_db)
		storage.load_configuration(None)
		container = storage.get_container(seq_id, 0)
		container.load_header()
		blocks = container.list_blocks()
		data_blocks = [b for b in blocks if b[2] == Container.CODE_DATA]
		self.assertEqual(block_digest, data_blocks[0][0])
	def test_new_containers_visible(self):
		"""Test that the new containers appearing in all the sequences are visible"""
		# Create two storages at the same place
		storage1 = Storage.DirectoryStorage(0, self.config_db)
		storage1.configure(self.CONFIGURATION, None)
		storage1.make_active()
		seq_id1 = storage1.get_active_sequence_id()
		config_db2 = self.env.get_database_btree("b", None, None)
		storage2 = Storage.DirectoryStorage(0, config_db2)
		storage2.configure(self.CONFIGURATION, None)
		storage2.make_active()
		seq_id2 = storage2.get_active_sequence_id()
		self.assert_(seq_id1 != seq_id2)
		# Create a container in each storage, make sure the containers are mutually visible
		c1 = storage1.create_container()
		c1.finish_dump()
		c1.upload()
		c2 = storage2.create_container()
		c2.finish_dump()
		c2.upload()
		# Reload the storages
		class Handler:
			def __init__(self):
				self.containers = []
			def report_new_container(self, container):
				self.containers.append((container.get_sequence_id(),
										container.get_index()))
			def get_containers(self):
				return self.containers
		handler1 = Handler()
		storage1.load_sequences(handler1)
		self.assertEqual([(seq_id2, c2.index)], handler1.get_containers())
		handler2 = Handler()
		storage2.load_sequences(handler2)
		self.assertEqual([(seq_id1, c1.index)], handler2.get_containers())
	def test_new_containers_in_active_sequence_caught(self):
		"""Test that if new containers appear unexpectedly in the active sequence,
		it is actually discovered"""
		storage1 = Storage.DirectoryStorage(0, self.config_db)
		storage1.configure(self.CONFIGURATION, None)
		storage1.make_active()
		seq_id1 = storage1.get_active_sequence_id()
		config_db2 = self.env.get_database_btree("b", None, None)
		storage2 = Storage.DirectoryStorage(0, config_db2)
		storage2.configure(self.CONFIGURATION, None)
		storage2.make_active()
		storage2.active_sequence_id = seq_id1
		c = storage2.create_container()
		c.finish_dump()
		c.upload()
		try:
			storage1.load_sequences(None)
		except:
			pass
		else:
			self.fail("Expected load_sequences to discover the unexpected container")
