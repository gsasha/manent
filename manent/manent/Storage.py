import base64
import os
import re
import shutil
import cStringIO as StringIO
import traceback

import Container
import utils.IntegerEncodings as IE

HEADER_EXT = "mnnh"
HEADER_EXT_TMP = "mnnhtmp"
BODY_EXT = "mnnb"
BODY_EXT_TMP = "mnnbtmp"

def _instantiate(config_db, storage_type, index):
	if storage_type == "directory":
		return DirectoryStorage(index, config_db)
	elif storage_type == "mail":
		return MailStorage()
	elif storage_type == "ftp":
		return FTPStorage(FTPHandler)
	elif storage_type == "sftp":
		return FTPStorage(SFTPHandler)
	elif storage_type == "__mock__":
		return MemoryStorage(index, config_db)
	else:
		raise Exception("Unknown storage_type type" + storage_type)
	
def create_storage(config_db, storage_type, index, params,
		new_container_handler):
	config_db["STORAGE.TYPE.%d" % index] = storage_type
	storage = _instantiate(config_db, storage_type, index)
	storage.configure(params, new_container_handler)
	return storage

def load_storage(config_db, index, new_container_handler):
	storage_type = config_db["STORAGE.TYPE.%d" % index]
	storage = _instantiate(config_db, storage_type, index)
	storage.load_configuration(new_container_handler)
	return storage

class Storage:
	def __init__(self, index, config_db):
		self.index = index
		self.config_db = config_db
		self.sequences = {}
		self.active_sequence_id = None
	
	# How do we know for a given storage if it is just created or rescanned?
	# Ah well, each storage stores its data in the shared db!
	
	def _key(self, suffix):
		return "STORAGE.%d.%s" % (self.index, suffix)
	#
	# Loading
	#
	def configure(self, config, new_container_handler):
		for key, val in config.iteritems():
			self.config_db[self._key('CONFIG.'+key)] = val
		
		self.config = config
		self.load_sequences(new_container_handler)
	def load_configuration(self, new_container_handler):
		self.config = self.get_config()
		self.load_sequences(new_container_handler)
	def get_config(self):
		PREFIX = self._key('CONFIG.')
		PREFIX_len = len(PREFIX)
		config = {}
		for key, val in self.config_db.iteritems_prefix(PREFIX):
			config[key[PREFIX_len:]] = val
		return config

	def get_password(self):
		if self.config.has_key('password'):
			return self.config['password']
		return None

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
		# Report the new container found
		for seq_id, index in new_header_files.iterkeys():
			if new_body_files.has_key((seq_id, index)):
				new_container_handler.report_new_container(seq_id, index)
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
	def get_container(self, sequence_id, index):
		container = Container.Container(self)
		container.start_load(sequence_id, index)
		return container

	def load_container_header(self, sequence_id, index):
		pass
	def load_container_body(self, sequence_id, index):
		pass
	def upload_container(self, sequence_id, index, header_file, body_file):
		pass
	
	def rescan(self):
		# TODO: Return the list of newly appeared containers
		self.fail()
	#
	# Block parameters
	#
	def blockSize(self):
		return 256*1024
	
	def compression_block_size(self):
		return 2*1024*1024


class FTPStorage(Storage):
	"""
	Handler for a FTP site storage
	"""
	def __init__(self, RemoteHandlerClass):
		Storage.__init__(self)
		self.RemoteHandlerClass = RemoteHandlerClass
		self.up_bw_limiter = BandwidthLimiter(15.0E3)
		self.down_bw_limiter = BandwidthLimiter(10000.0E3)
	def init(self, backup, txn_handler, params):
		Storage.init(self, backup, txn_handler)
		(host, user, password, path) = params
		self.fs_handler = self.RemoteHandlerClass(host, user, password, path)
	def container_size(self):
		return 4<<20
	def load_container_header(self, index, filename):
		print "Loading header for container", index, "     "
		remote_filename = self.compute_header_filename(index)
		self.fs_handler.download(
			FileWriter(filename,self.down_bw_limiter), remote_filename)
	
	def load_container_data(self,index,filename):
		print "Loading body for container", index, "     "
		remote_filename = self.compute_body_filename(index)
		self.fs_handler.download(
			FileWriter(filename,self.down_bw_limiter),
			remote_filename)
	
	def upload_container(self,index,header_file_name,body_file_name):
		remote_header_file_name = self.compute_header_filename(index)
		remote_body_file_name = self.compute_body_filename(index)
		self.fs_handler.upload(
			FileReader(header_file_name,self.up_bw_limiter),
			remote_header_file_name)
		self.fs_handler.upload(
			FileReader(body_file_name,self.up_bw_limiter),
			remote_body_file_name)
	
	def list_container_files(self):
		print "Scanning containers:"
		file_list = self.fs_handler.list_files()
		print "listed files", file_list
		return file_list

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
	def container_size(self):
		return 1<<10
	def list_container_files(self):
		return self.files.keys()
	def open_header_file(self, sequence_id, index):
		return StringIO.StringIO()
	def open_body_file(self, sequence_id, index):
		return StringIO.StringIO()
	def upload_container(self, sequence_id, index, header_file, body_file):
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		self.files[header_file_name] = header_file.getvalue()
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		self.files[body_file_name] = body_file.getvalue()
	def load_container_header(self, sequence_id, index):
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		return StringIO.StringIO(self.files[header_file_name])
	def load_container_body(self, sequence_id, index):
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT)
		return StringIO.StringIO(self.files[body_file_name])

class DirectoryStorage(Storage):
	"""
	Handler for a simple directory.
	"""
	def __init__(self, index, config_db):
		Storage.__init__(self, index, config_db)
	def configure(self, params, new_container_handler):
		Storage.configure(self, params, new_container_handler)
	def load_configuration(self, new_container_handler):
		Storage.load_configuration(self, new_container_handler)
	def get_path(self):
		return self.config["path"]
	def container_size(self):
		#return 4<<20
		return 4<<20
	def reconstruct_containers(self):
		#print "Scanning containers:", self.get_path()
		container_files = {}
		container_data_files = {}
		for file in os.listdir(self.get_path()):
			container_index = self.backup.global_config.container_index(
				file,self.backup.label,"")
			if container_index != None:
				container_files[container_index] = file
			container_index = self.backup.global_config.container_index(
				file,self.backup.label,".data")
			if container_index != None:
				container_data_files[container_index] = file
		max_container = 0
		for (index, file) in container_files.iteritems():
			print "  ", index, "\t", file,
			if container_data_files.has_key(index):
				print "\t", container_data_files[index]
			else:
				print
			if max_container<index:
				max_container = index
		for index in range(0,max_container+1):
			self.containers.append(None)
		self.containers_db["Containers"] = str(len(self.containers))
		
		print "Loading %d containers:" % max_container
		for (index, file) in container_files.iteritems():
			if not container_data_files.has_key(index):
				print "Container", index, "has no data file :("
				continue
			container = self.load_container(index)
			self.containers[index] = container
	def list_container_files(self):
		return os.listdir(self.get_path())
	def open_header_file(self, sequence_id, index):
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT_TMP)
		header_file_path = os.path.join(self.get_path(), header_file_name)
		return open(header_file_path, "w+")
	def open_body_file(self, sequence_id, index):
		body_file_name = self.encode_container_name(sequence_id, index, BODY_EXT_TMP)
		body_file_path = os.path.join(self.get_path(), body_file_name)
		return open(body_file_path, "w+")
	def upload_container(self, sequence_id, index, header_file, body_file):
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
		#print "Loading header for container",\
		  #base64.urlsafe_b64encode(sequence_id), index, "     "
		header_file_name = self.encode_container_name(sequence_id, index, HEADER_EXT)
		header_file_path = os.path.join(self.get_path(), header_file_name)
		return open(header_file_path, "r")
	def load_container_data(self,index):
		#print "Loading body for container",\
		  #base64.urlsafe_b64include(sequence_id), index, "     "
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

