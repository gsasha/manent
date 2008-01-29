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
import utils.RemoteFSHandler as RemoteFSHandler

HEADER_EXT = "mhd"
HEADER_EXT_TMP = "mhd-tmp"
BODY_EXT = "mbd"
BODY_EXT_TMP = "mbd-tmp"

def _instantiate(config_db, storage_type, index):
	if storage_type == "directory":
		return DirectoryStorage(index, config_db)
	elif storage_type == "mail":
		return MailStorage(index, config_db)
	elif storage_type == "ftp":
		return FTPStorage(index, config_db, RemoteFSHandler.FTPHandler)
	elif storage_type == "sftp":
		return FTPStorage(index, config_db, RemoteFSHandler.SFTPHandler)
	elif storage_type == "__mock__":
		return MemoryStorage(index, config_db)
	else:
		raise Exception("Unknown storage_type type" + storage_type)
	
def create_storage(db_manager, txn_manager, index, params, new_container_handler):
	config_db = db_manager.get_database_btree("config.db",
		"storage.%d" % index, txn_manager)
	storage_type = params['type']
	config_db["TYPE"] = storage_type
	storage = _instantiate(config_db, storage_type, index)
	storage.configure(params, new_container_handler)
	return storage

def load_storage(db_manager, txn_manager, index, new_container_handler):
	config_db = db_manager.get_database_btree("config.db",
		"storage.%d" % index, txn_manager)
	storage_type = config_db["TYPE"]
	storage = _instantiate(config_db, storage_type, index)
	storage.load_configuration(new_container_handler)
	return storage

class Storage:
	def __init__(self, index, config_db):
		self.config_db = config_db
		self.index = index
		self.sequences = {}
		self.active_sequence_id = None
		self.aside_header_file = None
		self.aside_body_file = None
	
	# How do we know for a given storage if it is just created or rescanned?
	# Ah well, each storage stores its data in the shared db!
	
	def _key(self, suffix):
		return "%s" % (suffix)
	#
	# Loading
	#
	def configure(self, config, new_container_handler):
		for key, val in config.iteritems():
			self.config_db[self._key('CONFIG.'+key)] = val
			#print "setting config_db[%s]=%s" % (self._key('CONFIG.'+key), val)
		
		self.config = config
		self.load_sequences(new_container_handler)
	def load_configuration(self, new_container_handler):
		self.config = self.get_config()
		self.load_sequences(new_container_handler)
	def get_config(self):
		PREFIX = self._key('CONFIG.')
		print "Getting config, prefix=", PREFIX
		PREFIX_len = len(PREFIX)
		config = {}
		for key, val in self.config_db.iteritems_prefix(PREFIX):
			config[key[PREFIX_len:]] = val
		return config

	def get_encryption_key(self):
		if self.config.has_key('encryption_key'):
			return self.config['encryption_key']
		return None
	def get_index(self):
		return self.index

	# Data structure stored in a database for a specific storage:
	# storage.%d.active_sequence - the sequence to which new containers
	#                              are added
	# storage.%d.$sequence.index  - the index of the sequence with the given
	#     storage id. Used to determine if the sequence has been loaded already
	# storage.%d.$sequence.next_container - the index of the last known
	#     container in the given sequence. Used to determine which new
	#     containers have appeared for the sequence
	def make_active(self):
		# If this storage is not active, create a sequence.
		if self.active_sequence_id is None:
			self.create_sequence()
	def is_active(self):
		return self.active_sequence_id is not None
	def create_sequence(self):
		#print "Creating sequence"
		self.active_sequence_id = os.urandom(12)
		self.active_sequence_next_index = 0
		NEXT_INDEX_KEY = self._key(self.active_sequence_id+".next_index")
		self.config_db[self._key("active_sequence")] = self.active_sequence_id
		self.config_db[NEXT_INDEX_KEY] = str(self.active_sequence_next_index)
		return self.active_sequence_id
	def get_active_sequence_id(self):
		return self.active_sequence_id
	def get_next_index(self):
		index = self.active_sequence_next_index
		self.active_sequence_next_index += 1
		NEXT_INDEX_KEY = self._key(self.active_sequence_id+".next_index")
		self.config_db[NEXT_INDEX_KEY] = str(self.active_sequence_next_index)
		return index
	def load_sequences(self, new_container_handler):
		# Load previously known sequences
		SEQ_PREFIX = self._key(".sequences.")
		for key, value in self.config_db.iteritems_prefix(SEQ_PREFIX):
			seq_id = key[len(SEQ_PREFIX):]
			self.sequences[seq_id] = int(value)
		# Load the data from the storage location
		container_files = self.list_container_files()
		new_header_files = {}
		new_body_files = {}
		for file in container_files:
			seq_id, index, extension = self.decode_container_name(file)
			if not self.sequences.has_key(seq_id) or self.sequences[seq_id] < index:
				if extension == HEADER_EXT:
					new_header_files[(seq_id, index)] = 1
				elif extension == BODY_EXT:
					new_body_files[(seq_id, index)] = 1
		for file in container_files:
			seq_id, index, extension = self.decode_container_name(file)
			if seq_id is None:
				continue
			if seq_id == self.active_sequence_id and index >= self.active_sequence_next_index:
				raise Exception("Unexpected container: nobody else should be adding " +
			                    "containers to this sequence")
			if self.sequences.has_key(seq_id):
				self.sequences[seq_id] = max(self.sequences[seq_id], index)
			else:
				self.sequences[seq_id] = index
		# Update the sequences info in the database
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
		# Reload the active sequence
		if self.config_db.has_key(self._key("active_sequence")):
			self.active_sequence_id = self.config_db[self._key("active_sequence")]
			NEXT_INDEX_KEY = self._key(self.active_sequence_id+".next_index")
			self.active_sequence_next_index = int(self.config_db[NEXT_INDEX_KEY])
	def get_sequence_ids(self):
		return self.sequences.keys()
	def close(self):
		pass
	def info(self):
		pass

	def encode_container_name(self, sequence_id, index, extension):
		try:
			return "manent.%s.%s.%s" % (base64.urlsafe_b64encode(sequence_id),
				IE.ascii_encode_int_varlen(index), extension)
		except:
			traceback.print_exc()
			raise
	def decode_container_name(self, name):
		name_re = re.compile("manent.([^.]+).([^.]+).([^.]+)")
		match = name_re.match(name)
		if not match:
			return (None, None, None)
		try:
			sequence_id = base64.urlsafe_b64decode(match.groups()[0])
			index = IE.ascii_decode_int_varlen(match.groups()[1])
			extension = match.groups()[2]
			return (sequence_id, index, extension)
		except:
			# File name unparseable. Can be junk coming from something else
			return (None, None, None)
	#
	# Container management
	#
	def create_container(self):
		if self.active_sequence_id is None:
			raise Exception("Can't create a container for an inactive storage")
		container = Container.Container(self)
		index = self.get_next_index()
		container.start_dump(self.active_sequence_id, index)
		self.sequences[self.active_sequence_id] = index
		self.config_db[self._key(".sequences." + self.active_sequence_id)] = str(index)
		return container
	# Creates a container that is used for aside purposes, i.e., a temporary
	# holder of non-data blocks
	def create_aside_container(self):
		if self.active_sequence_id is None:
			raise Exception("Can't create a container for an inactive storage")
		container = Container.Container(self)
		index = None
		container.start_dump(self.active_sequence_id, index)
		return container
	def import_aside_container(self, container):
		#TODO: implement this
		index = self.get_next_index()
		container.override_index(index)
		self.sequences[self.active_sequence_id] = index
		self.config_db[self._key(".sequences." + self.active_sequence_id)] = str(index)
	def get_container(self, sequence_id, index):
		container = Container.Container(self)
		container.start_load(sequence_id, index)
		return container
	# Get the aside container, opened for reading
	def get_aside_container(self):
		container = Container.Container(self)
		index = None
		container.start_load(self.active_sequence_id, index)
		return container

	def open_aside_container_header(self):
		assert self.aside_header_file is None
		self.aside_header_file = tempfile.TemporaryFile()
		return self.aside_header_file
	def open_aside_container_body(self):
		assert self.aside_body_file is None
		self.aside_body_file = tempfile.TemporaryFile()
		return self.aside_body_file
	def load_aside_container_header(self):
		assert self.aside_header_file is not None
		file = self.aside_header_file
		file.seek(0)
		self.aside_header_file = None
		return file
	def load_aside_container_body(self):
		assert self.aside_body_file is not None
		file = self.aside_body_file
		file.seek(0)
		self.aside_body_file = None
		return file
	def load_container_header(self, sequence_id, index):
		print "SELF:", self
		raise Exception("load_container_header is abstract")
	def load_container_body(self, sequence_id, index):
		print "SELF:", self
		raise Exception("load_container_body is abstract")
	def upload_container(self, sequence_id, index, header_file, body_file):
		raise Exception("upload_container is abstract")
	
	#
	# Block parameters
	#
	def get_block_size(self):
		return 256*1024
	
	def get_compression_block_size(self):
		return 2*1024*1024

# Used only for testing
class MemoryStorage(Storage):
	# NOTE: global var. It doesn't matter, since it's for testing only.
	files = {}
	def __init__(self, index, config_db):
		Storage.__init__(self, index, config_db)
	def configure(self, params, new_container_handler):
		Storage.configure(self, params, new_container_handler)
	def load_configuration(self, new_container_handler):
		Storage.load_configuration(self, new_container_handler)
	def get_cur_files(self):
		if not self.files.has_key(self.config['key']):
			self.files[self.config['key']] = {}
		return self.files[self.config['key']]
	def container_size(self):
		return 1<<10
	def list_container_files(self):
		return self.get_cur_files().keys()
	def open_header_file(self, sequence_id, index):
		return StringIO.StringIO()
	def open_body_file(self, sequence_id, index):
		return StringIO.StringIO()
	def upload_container(self, sequence_id, index, header_file, body_file):
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		self.get_cur_files()[header_file_name] = header_file.getvalue()
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		self.get_cur_files()[body_file_name] = body_file.getvalue()
	def load_container_header(self, sequence_id, index):
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		return StringIO.StringIO(self.get_cur_files()[header_file_name])
	def load_container_body(self, sequence_id, index):
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		return StringIO.StringIO(self.get_cur_files()[body_file_name])

class FTPStorage(Storage):
	"""
	Handler for a FTP site storage
	"""
	def __init__(self, index, config_db, RemoteHandlerClass):
		Storage.__init__(self, index, config_db)
		self.RemoteHandlerClass = RemoteHandlerClass
		self.fs_handler = None
		#self.up_bw_limiter = BandwidthLimiter(15.0E3)
		#self.down_bw_limiter = BandwidthLimiter(10000.0E3)
	def configure(self, params, new_container_handler):
		Storage.configure(self, params, new_container_handler)
	def load_configuration(self, new_container_handler):
		Storage.load_configuration(self, new_container_handler)
		print "Loaded directory storage configuration", self.config

	def get_fs_handler(self):
		if self.fs_handler is None:
			self.fs_handler = self.RemoteHandlerClass(self.get_host(),
				self.get_user(), self.get_password(), self.get_path())
		return self.fs_handler

	def get_host(self):
		return self.config["host"]
	
	def get_user(self):
		return self.config["user"]
	
	def get_password(self):
		return self.config["password"]
	
	def get_path(self):
		return self.config["path"]

	def container_size(self):
		return 4<<20
	
	def list_container_files(self):
		print "Scanning containers:"
		file_list = self.get_fs_handler().list_files()
		print "listed files", file_list
		return file_list

	def open_header_file(self, sequence_id, index):
		print "Starting container header", base64.urlsafe_b64encode(sequence_id), index
		return tempfile.TemporaryFile(dir=Config.paths.staging_area())
	def open_body_file(self, sequence_id, index):
		print "Starting container body", base64.urlsafe_b64encode(sequence_id), index
		return tempfile.TemporaryFile(dir=Config.paths.staging_area())
	
	def load_container_header(self, sequence_id, index):
		print "Loading container header", base64.urlsafe_b64encode(sequence_id), index
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		filehandle = tempfile.TemporaryFile(dir=Config.paths.staging_area())
		self.get_fs_handler().download(filenandle, header_file_name)
		filehandle.seek(0)
		return filehandle
	def load_container_body(self, sequence_id, index):
		print "Loading container body", base64.urlsafe_b64encode(sequence_id), index
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		filehandle = tempfile.TemporaryFile(dir=Config.paths.staging_area())
		self.get_fs_handler().download(filenandle, body_file_name)
		filehandle.seek(0)
		return filehandle
	
	#def load_container_header(self, index, filename):
		#print "Loading header for container", index, "     "
		#remote_filename = self.compute_header_filename(index)
		#self.fs_handler.download(
			#FileWriter(filename,self.down_bw_limiter), remote_filename)
	
	#def load_container_data(self,index,filename):
		#print "Loading body for container", index, "     "
		#remote_filename = self.compute_body_filename(index)
		#self.fs_handler.download(
			#FileWriter(filename,self.down_bw_limiter),
			#remote_filename)

	def upload_container(self, sequence_id, index, header_file, body_file):
		print "Uploading container", base64.urlsafe_b64encode(sequence_id), index
		header_file_name_tmp = self.encode_container_name(sequence_id, index, HEADER_EXT_TMP)
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		body_file_name_tmp = self.encode_container_name(sequence_id, index, BODY_EXT_TMP)
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		# Upload the files
		header_file.seek(0)
		self.get_fs_handler().upload(header_file, header_file_name_tmp)
		body_file.seek(0)
		self.get_fs_handler().upload(body_file, body_file_name_tmp)
		# Rename the tmp files to permanent ones
		self.get_fs_handler().rename(header_file_name_tmp, header_file_name)
		self.get_fs_handler().rename(body_file_name_tmp, body_file_name)
		#TODO: return to using FileReader, FileWriter
		# Remove the write permission off the permanent files
		self.get_fs_handler().chmod(header_file_name, 0444)
		self.get_fs_handler().chmod(body_file_name, 0444)
	#def upload_container(self,index,header_file_name,body_file_name):
		#remote_header_file_name = self.compute_header_filename(index)
		#remote_body_file_name = self.compute_body_filename(index)
		#self.fs_handler.upload(
			#FileReader(header_file_name, self.up_bw_limiter),
			#remote_header_file_name)
		#self.fs_handler.upload(
			#FileReader(body_file_name, self.up_bw_limiter),
			#remote_body_file_name)

class DirectoryStorage(Storage):
	"""
	Handler for a simple directory.
	"""
	def __init__(self, index, config_db):
		Storage.__init__(self, index, config_db)
	def configure(self, params, new_container_handler):
		print "Configuring directory storage with parameters", params
		Storage.configure(self, params, new_container_handler)
	def load_configuration(self, new_container_handler):
		Storage.load_configuration(self, new_container_handler)
		print "Loaded directory storage configuration", self.config
	def get_path(self):
		return self.config["path"]
	def container_size(self):
		#return 4<<20
		return 4<<20
	def list_container_files(self):
		return os.listdir(self.get_path())
	def open_header_file(self, sequence_id, index):
		print "Starting container header", base64.urlsafe_b64encode(sequence_id), index
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT_TMP)
		header_file_path = os.path.join(self.get_path(), header_file_name)
		return open(header_file_path, "w+")
	def open_body_file(self, sequence_id, index):
		print "Starting container body", base64.urlsafe_b64encode(sequence_id), index
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT_TMP)
		body_file_path = os.path.join(self.get_path(), body_file_name)
		return open(body_file_path, "w+")
	def upload_container(self, sequence_id, index, header_file, body_file):
		print "Uploading container", base64.urlsafe_b64encode(sequence_id), index
		header_file_name_tmp = self.encode_container_name(sequence_id, index, HEADER_EXT_TMP)
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		body_file_name_tmp = self.encode_container_name(sequence_id, index, BODY_EXT_TMP)
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		header_file_path_tmp = os.path.join(self.get_path(), header_file_name_tmp)
		header_file_path = os.path.join(self.get_path(), header_file_name)
		body_file_path_tmp = os.path.join(self.get_path(), body_file_name_tmp)
		body_file_path = os.path.join(self.get_path(), body_file_name)
		# Rename the tmp files to permanent ones
		shutil.move(header_file_path_tmp, header_file_path)
		shutil.move(body_file_path_tmp, body_file_path)
		# Remove the write permission off the permanent files
		os.chmod(header_file_path, 0444)
		os.chmod(body_file_path, 0444)
	def load_container_header(self, sequence_id, index):
		print "Loading container header", base64.urlsafe_b64encode(sequence_id), index
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		header_file_path = os.path.join(self.get_path(), header_file_name)
		return open(header_file_path, "r")
	def load_container_body(self, sequence_id, index):
		print "Loading container body", base64.urlsafe_b64encode(sequence_id), index
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		body_file_path = os.path.join(self.get_path(), body_file_name)
		return open(body_file_path, "r")

import smtplib
import email.Encoders as Encoders
import email.Message as Message
import email.MIMEAudio as MIMEAudio
import email.MIMEBase as MIMEBase
import email.MIMEMultipart as MIMEMultipart
import email.MIMEImage as MIMEImage
import email.MIMEText as MIMEText

class MailStorage(Storage):
	"""
	Handler for gmail container.
	Needs a list of gmail addresses.
	"""
	def __init__(self):
		Storage.__init__(self)
		self.accounts = []
	def init(self,backup,txn_handler,params):
		Storage.init(self,backup,txn_handler)
		self.backup = backup
		self.username = params[0]
		self.password = params[1]
		self.quota = int(params[2])
	def configure(self,username,password,quota):
		self.add_account(username,password,quota)
	def load(self,filename):
		file = open(filename, "rb")
		Storage.load(self,file)
		configLine = file.readline()
		(username,password,quota) = re.split("\s+",configLine)[0:3]
		self.add_account(username,password,quota)
	def save(self,filename):
		file = open(filename,"wb")
		Storage.save(self,file)
		account = self.accounts[0]
		file.write("%s %s %s" %
			(account["user"],account["pass"],account["quota"]))
	def add_account(self,username,password,quota):
		self.accounts.append(
			{"user":username, "pass":password, "quota":quota, "used":0})
	def container_size(self):
		return 2<<20
	def save_container(self,container):
		print "Saving container"

		s = smtplib.SMTP()
		s.set_debuglevel(1)
		print "connecting"
		#s.connect("gmail-smtp-in.l.google.com")
		s.connect("alt2.gmail-smtp-in.l.google.com")
		#s.connect("smtp.gmail.com", 587)
		print "starting tls"
		s.ehlo("www.manent.net")
		s.starttls()
		s.ehlo("www.manent.net")
		print "logging in"
		
		print "sending header in mail"
		#s.set_debuglevel(0)
		header_msg = MIMEMultipart()
		header_msg["Subject"] = "manent.%s.%s.header" % (
			self.backup.label, str(container.index))
		#header_msg["To"] = "gsasha@gmail.com"
		#header_msg["From"] = "gsasha@gmail.com"
		header_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		header_file = open(os.path.join(
			self.backup.global_config.staging_area(),filename), "rb")
		header_attch.set_payload(header_file.read())
		header_file.close()
		Encoders.encode_base64(header_attch)
		header_msg.attach(header_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com",
			"gsasha.manent1@gmail.com", header_msg.as_string())
		s.set_debuglevel(1)
		
		print "sending data in mail"
		data_msg = MIMEMultipart()
		data_msg["Subject"] = "manent.%s.%s.data" % (self.backup.label,
			str(container.index))
		data_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		data_file = open(
			os.path.join(self.backup.global_config.staging_area(),
				filename+".data"), "rb")
		data_attch.set_payload(data_file.read())
		data_file.close()
		Encoders.encode_base64(data_attch)
		data_msg.attach(data_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com",
			"gsasha.manent1@gmail.com", data_msg.as_string())
		s.set_debuglevel(1)
		print "all done"
		
		s.close()
		print header_msg.as_string()

class OpticalStorage(Storage):
	"""
	Handler for optical container.
	Can be one of: CD-650, CD-700, DVD, DVD-DL, BLURAY :)
	"""
	CONTAINER_TYPES = {
		"CD-650" : 650<<20,
		"CD-700" : 700<<20,
		"DVD"    : 4600<<20,
		"DVD-DL" : 8500<<20,
		"BLURAY" : 26000<<20,
		}
	def __init__(self):
		self.containerType = NONE
		self.containers = []
		raise "not implemented"
	def configure(self,params):
		(containerType,) = params
		self.containerType = containerType
		if not CONTAINER_TYPES.has_key(self.containerType):
			print "Unknown container type", self.containerType
			exit(1)
	def load(self,filename):
		file = open(filename, "rb")
		Storage.load(self,file)
		containerType = file.readline()
		for line in file:
			self.containers.append(line)
	def save(self,file):
		file = open(filename,"wb")
		Storage.save(self,file)
		file.write(self.containerType+"\n")
		for container in self.containers:
			file.write(container+"\n")
	def container_size(self):
		return CONTAINER_TYPES[self.containerType]

