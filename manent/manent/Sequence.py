#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging
import os
import re
import shutil
import cStringIO as StringIO
import tempfile
import traceback

import Config
import Container
import utils.IntegerEncodings as IE
import Storage

class Sequence:
	"""Represents and manages a single sequence.
	   Containers are numbered sequentially from 0.
	   Containers are loaded in continuous chunks."""
	def __init__(self, storage, seq_id, generate_summary):
		self.storage = storage
		self.sequence_id = seq_id
		self.generate_summary = generate_summary
		if self._get_config_db().has_key(self._key("next_index")):
			self.next_container_idx = int(
				self._get_config_db()[self._key("next_index")])
		else:
			self.next_container_idx = 0
			self._get_config_db()[self._key("next_index")] = (
				self.next_container_idx)
		self.new_summary_containers = []
		self.new_container_headers = []
		self.new_container_bodies = []
		self.summary_headers_num = 0
		self.summary_headers_len = 0
		self.summary_header_last_idx = None
	def set_active(self, active):
		self.is_active = active
	def get_next_index(self):
		index = self.next_container_idx
		self.next_container_idx += 1
		NEXT_INDEX_KEY = self._key("next_index")
		self._get_config_db()[NEXT_INDEX_KEY] = str(self.next_container_idx)
		return index
	def register_new_file(self, index, extension):
		if index < self.next_container_idx:
			# We already have registered this container
			return
		if extension == Storage.SUMMARY_HEADER_EXT:
			self.new_summary_containers.append(index)
		elif extension == Storage.HEADER_EXT:
			self.new_container_headers.append(index)
		else:
			self.new_container_bodies.append(index)
	def process_new_files(self):
		# 1. read all new headers into loaded_headers_db.
		# 2. notify the client about the newly loaded headers.
		# 3. clean up loaded_headers_db, since these headers are
		#    no longer going to be used.
		self.new_summary_containers.sort()
		self.new_container_headers.sort()
		self.new_container_bodies.sort()
		print "New headers  :", self.new_container_headers
		print "New bodies   :", self.new_container_bodies
		print "New summaries:", self.new_summary_containers

		self.read_summary_containers()
		self.analyze_new_containers()
		if self.generate_summary:
			self.write_summary_containers()
	def read_summary_containers(self):
		# We load in reverse order since later summary containers are likely
		# larger and therefore are more efficient to load.
		next_loaded_container = self.next_container_idx
		for index in reversed(self.new_summary_containers):
			if self.next_loaded_container > index:
				print "but it says nothing new"
				continue
			self.summary_headers_num += 1
			# Ok, this header file tells us something new. Let's load it.
			print "Loading container header", index, "from summary", file
			stream = self._load_container_header_summary(index)
			self.last_loaded_container = max(self.ast_loaded_container, index)
			last_index = None
			while True:
				header_index = IE.binary_read_int_varlen(stream)
				if header_index is None:
					# No more data in the summary container
					break
				# Check that the containers in the summary header progress
				# correctly.
				assert last_index is None or last_index < header_index
				last_index = header_index
				# Load the header itself
				print "  --> loading header", header_index
				header_data_len = IE.binary_read_int_varlen(stream)
				assert header_data_len is not None
				header_data = stream.read(header_data_len)
				assert len(header_data) == header_data_len
				self.summary_headers_len += header_data_len

				# Write down the header
				if (index < next_loaded_container):
					print "  --> But the header already known"
					continue
				# Test that we haven't loaded this container yet.
				if self._get_loaded_headers_db().has_key(self._key(str(index))):
					print "  --> But the header is loaded from another summary"
					continue
				# Ok, this header is really new. Keep it.
				#print "\n  *********** importing header", index
				#print "  *********** setting loaded_headers_db[", header_name,
				#print "to", header_data
				self._get_loaded_headers_db()[header_name] = header_data
	def analyze_new_containers(self):
		# Scan container files to find the newly appeared ones
		# 1. Import the data from all new summary containers.
		#    Check that if we have a summary container, we have
		#    headers up to that number.
		# 2. Report all new headers.
		# 3. Advance last index for the header.

		# Find the number of sequentially numbered container headers
		# TODO: make sure to first upload data, and then header container!
		next_seen_header = self.next_container_idx
		for header_idx in sorted(self.new_container_headers):
			if next_seen_header == header_idx:
				next_seen_header = header_idx + 1
		next_seen_body = self.next_container_idx
		
		container_bodies_dict = {}
		for body_idx in self.new_container_bodies:
			container_bodies_dict[body_idx] = True
		if self.generate_summary:
			for header_idx in self.new_container_headers:
				if (header_idx > self.next_container_idx and
					not self._get_loaded_headers_db().has_key(
						self._key(str(header_idx)))):
					file = self._load_container_header(header_idx)
					self.add_summary_header(header_idx, file)
				if not container_bodies_dict.has_key(header_idx):
					print "Warning: No body corresponding to header",
					print header_idx
		return
		for file in container_files:
			seq_id, index, extension = decode_container_name(file)
			if seq_id is None:
				continue
			if (seq_id == self.active_sequence_id and
				index >= self.active_sequence_next_index):
				raise Exception("Unexpected container: "
				                "nobody else should be adding "
			                    "containers to this sequence")
			if self.sequences.has_key(seq_id):
				self.sequences[seq_id] = max(self.sequences[seq_id], index)
			else:
				self.sequences[seq_id] = index
		# If we're generating summary headers, write them out now.
		if unsummarized_headers is not None:
			for seq_id, index in unsummarized_headers.iteritems():
				if seq_id != self.active_sequence_id:
					self.write_summary_header(index)
				
		for key, value in self.sequences.iteritems():
			config_k = self._key(".sequences." + key)
			if self.config_db.has_key(config_k) and\
			   self.config_db[config_k] == str(value):
				continue
			self.config_db[config_k] = str(value)
		# Report the new containers found
		for seq_id, index in sorted(new_header_files.iterkeys()):
			if new_body_files.has_key((seq_id, index)):
				container = self.get_container(seq_id, index)
				new_container_handler.report_new_container(container)
	def add_summary_header(self, index, file):
		file.seek(0)
		file_contents = file.read()
		self._get_summary_headers_db()[str(index)] = file_contents
		assert len(file_contents) > 0
		self.summary_headers_len += len(str(index)) + len(file_contents)
		self.summary_headers_num += 1
		self.summary_header_last_idx = index
		if self.summary_headers_len >= self._get_container_size():
			self.write_summary_header(index)
	def flush_summary(self):
		self.write_summary(self.summary_header_last_idx)
	def write_summary(self, index):
		if self.summary_headers_num <= 1:
			print "Only one header in summary. Not writing summary header"
			return
		summary_file_name = Storage.encode_container_name(
			self.sequence_id, index, Storage.SUMMARY_HEADER_EXT)
		summary_file_name_tmp = Storage.encode_container_name(
			self.sequence_id, index, Storage.SUMMARY_HEADER_EXT_TMP)
		print "\nWriting summary header",
		print base64.b64encode(self.sequence_id), index,
		print "to file", summary_file_name
		tmpfile = tempfile.TemporaryFile()
		keys = []
		for key, value in self._get_summary_headers_db().iteritems():
			print "Adding header", key, len(key), len(value),
			keys.append(key)
			tmpfile.write(IE.binary_encode_int_varlen(len(key)))
			print base64.b16encode(IE.binary_encode_int_varlen(len(key))),
			print base64.b16encode(IE.binary_encode_int_varlen(len(value)))
			tmpfile.write(key)
			tmpfile.write(IE.binary_encode_int_varlen(len(value)))
			tmpfile.write(value)
		for key in keys:
			del self._get_summary_headers_db()[key]
		tmpfile.seek(0)
		self.upload_file(summary_file_name, summary_file_name_tmp, tmpfile)
		tmpfile.close()
		self.summary_headers_written += 1
		self.summary_headers_num = 0
	def _get_config_db(self):
		return self.storage.config_db
	def _get_loaded_headers_db(self):
		return self.storage.loaded_headers_db
	def _get_summary_headers_db(self):
		return self.storage.summary_headers_db
	def _get_container_size(self):
		return self.storage.container_size()
	def _key(self, suffix):
		return self.storage._key(self.sequence_id + suffix)
	def _load_container_header(self, index):
		return self.storage.load_container_header(self.sequence_id, index)
	def _load_container_header_sumary(self, index):
		# TODO: implement me
		return
