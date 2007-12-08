import base64
import re
import os

import utils.IntegerEncodings as IE

def create_storage(storage_type):
	if storage_type == "directory":
		return DirectoryStorage()
	elif storage_type == "mail":
		return MailStorage()
	elif storage_type == "ftp":
		return FTPStorage(FTPHandler)
	elif storage_type == "sftp":
		return FTPStorage(SFTPHandler)
	elif storage_type == "dummy don't use me!!!":
		return DummyStorage()
	else:
		raise Exception("Unknown storage_type type" + storage_type)

class Storage:
	def __init__(self, index, config_db):
		self.index = index
		self.config_db = config_db
		self.sequences = {}
	
	# How do we know for a given storage if it is just created or rescanned?
	# Ah well, each storage stores its data in the shared db!
	
	def get_prefix(self):
		return "STORAGE.%d." % self.index
	#
	# Loading
	#
	def configure(self, config):
		PREFIX = self.get_prefix() + 'CONFIG.'
		for key, val in config.iteritems():
			self.config_db[PREFIX+key] = val
		
		self.path = config["path"]
		self.load_sequences()
		self.create_sequence()
	def get_config(self):
		PREFIX = self.get_prefix() + 'CONFIG.'
		PREFIX_len = len(PREFIX)
		config = {}
		for key, val in self.config_db.iteritems_prefix(PREFIX):
			config[key[PREFIX_len:]] = val
		return config

	# Data structure stored in a database for a specific storage:
	# storage.%d.active_sequence - the sequence to which new containers
	#                              are added
	# storage.%d.$sequence.index  - the index of the sequence with the given
	#     storage id. Used to determine if the sequence has been loaded already
	# storage.%d.$sequence.next_container - the index of the last known
	#     container in the given sequence. Used to determine which new
	#     containers have appeared for the sequence
	def create_sequence(self):
		PREFIX = self.get_prefix()
		sequence_id = os.urandom(10)
		self.config_db[PREFIX+"active_sequence"] = sequence_id
		self.config_db[PREFIX+"%s.next_container"%(sequence_id)] = '0'
	def load_sequences(self):
		sequences = {}
		for file in self.list_container_files():
			seq_id, index, extension = self.decode_container_name(file)
			if self.sequences.has_key(seq_id):
				self.sequences[seq_id] = max(self.sequences[seq_id], index)
			else:
				self.sequences[seq_id] = index
		# TODO: put the sequences to self.sequences structure
		# TODO: report on the extra containers that have appeared
	def close(self):
		pass
	def info(self):
		pass

	def encode_container_name(self, sequence, index, extension):
		return "manent.%s.%s.%s" % (base64.urlsafe_b64encode(sequence),
			IE.ascii_encode_int_varlen(index), extension)
	def decode_container_name(self, name):
		name_re = re.compile("manent.([^.]+).([^.]+).([^.]+)")
		match = name_re.match(name)
		if not match:
			return (None, None, None)
		sequence = base64.urlsafe_b64decode(match.groups()[0])
		index = IE.ascii_decode_int_varlen(match.groups()[1])
		extension = match.groups()[2]

		return (sequence, index, extension)
	#
	# Reconstruction!
	#
	def reconstruct(self,handler):
		#
		# It is the specific implementation of Storage that knows how to
		# reconstruct the containers
		#
		container_files = self.list_container_files()
		container_header_files = {}
		container_body_files = {}
		for file in file_list:
			index = self.container_header_index(file,self.label)
			if index != None:
				container_header_files[index] = file
			index = self.container_body_index(file,self.label)
			if index != None:
				container_body_files[index] = file

		for index in sorted(container_header_files.keys()):
			if not container_body_files.has_key(index):
				print "Container %d has header but no index\n" % index
				continue
			container = Container(
				self,index,container_header_files[index],
				container_body_files[index])
			container.load_header()

			has_requested_blocks = False
			has_unrequested_blocks = False
			
			for digest,size,code in container.list_blocks():
				if handler.is_requested(digest,code):
					has_requested_blocks = True
				else:
					has_unrequested_blocks = True

			if has_requested_blocks:
				container.load_body()

			container.load_blocks(handler)
			if has_unrequested_blocks:
				container.TODO()
		
		for (index, file) in container_header_files.iteritems():
			print "  ", index, "\t", file,
			if container_data_files.has_key(index):
				print "\t", container_data_files[index]
			else:
				print
			if max_container < index:
				max_container = index
		for index in range(0, max_container + 1):
			self.containers.append(None)
		self.containers_db["Containers"] = str(len(self.containers))
		print " Before loading we have %d containers " % self.num_containers()
		print "Loading %d containers:" % max_container
		for (index, file) in container_files.iteritems():
			if not container_data_files.has_key(index):
				print "Container", index, "has no data file :("
				continue
			container = self.load_container(index)
			self.containers[index] = container
	#
	# Container management
	#
	def get_container(self,index):
		random_part = os.urandom(10)
		header_file_name = "/tmp/manent.container.%s.header" % random_part
		body_file_name   = "/tmp/manent.container.%s.body" % random_part
		return Container(self,index,header_file_name,body_file_name)

	def load_container_header(self,index,file_name):
		pass
	def load_container_body(self,index,file_name):
		pass
	def upload_container(self,index,header_file_name,body_file_name):
		pass
	
	def rescan(self):
		# TODO: Return the list of newly appeared containers
		pass
	#
	# Block parameters
	#
	def blockSize(self):
		return 256*1024
	
	def compression_block_size(self):
		return 2*1024*1024


from ftplib import FTP

class FTPStorage(Storage):
	"""
	Handler for a FTP site storage
	"""
	def __init__(self,RemoteHandlerClass):
		Storage.__init__(self)
		self.RemoteHandlerClass = RemoteHandlerClass
		self.up_bw_limiter = BandwidthLimiter(15.0E3)
		self.down_bw_limiter = BandwidthLimiter(10000.0E3)
	def init(self,backup,txn_handler,params):
		Storage.init(self,backup,txn_handler)
		(host,user,password,path) = params
		self.fs_handler = self.RemoteHandlerClass(host,user,password,path)
	def container_size(self):
		return 4<<20
	def load_container_header(self,index,filename):
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

class DirectoryStorage(Storage):
	"""
	Handler for a simple directory.
	"""
	def __init__(self, index, config_db):
		Storage.__init__(self, index, config_db)
		self.path = None
	def configure(self, params):
		Storage.configure(self, params)
		(path,) = params
		self.path = path
	def load_configuration(self):
		Storage.load_configuration(self)
	def container_size(self):
		#return 4<<20
		return 4<<20
       	def reconstruct_containers(self):
		print "Scanning containers:", self.path
		container_files = {}
		container_data_files = {}
		for file in os.listdir(self.path):
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
		return os.listdir(self.path)
	def load_container(self, index):
		print "Loading header for container", index, "     "
		container = Container(self.backup,index)

		filename = container.filename()
		staging_path = os.path.join(
			self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		container.load(os.path.join(self.path, filename))
		return container
	def load_container_data(self,index):
		print "Loading data for container", index, "     "
		container = Container(self.backup,index)

		filename = container.filename()+".data"
		staging_path = os.path.join(
			self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		return target_path
	def save_container(self,container):
		index = container.index
		
		filename = container.filename()
		staging_path = os.path.join(
			self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		
		if staging_path != target_path:
			shutil.move(staging_path, target_path)
			shutil.move(staging_path+".data", target_path+".data")

import smtplib
from email import Encoders
from email.Message import Message
from email.MIMEAudio import MIMEAudio
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEImage import MIMEImage
from email.MIMEText import MIMEText

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

