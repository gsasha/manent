
def create_storage(storage_type):
	if storage_type == "directory":
		return DirectoryStorage()
	elif storage_type == "mail":
		return MailStorage()
	elif storage_type == "ftp":
		return FTPStorage(FTPHandler)
	elif storage_type == "sftp":
		return FTPStorage(SFTPHandler)
	raise "Unknown storage_type type", storage_type

class Storage:
	def __init__(self):
		self.containers_db = None
	
	# How do we know for a given storage if it is just created or rescanned?
	# Ah well, each storage stores its data in the shared db!
	
	#
	# Loading
	#
	def init(self,db_config,label,txn_handler):
		self.db_config = db_config
		self.label = label
		self.containers_db = self.db_config.get_database(".containers.%d"%self.label,txn_handler)

		#
		# See if we are loading the db for the first time
		#
		if not self.containers_db.has_key("Containers"):
			self.containers_db["Containers"] = str(0)

		#
		# Load the existing increments and containers
		#
		self.containers = []
		for i in range(0,int(self.containers_db["Containers"])):
			self.containers.append(None)
	def close(self):
		if self.containers_db is not None:
			# It can be none if its initialization fails
			self.containers_db.close()
	def info(self):
		print "Containers:", len(self.containers)
			
		for i in range(0,len(self.containers)):
			container = self.get_container(i)
			container.info()
			self.containers[i] = None
	#
	# Increment management
	#
	def start_increment(self,base_index):
		if self.new_increment != None:
			raise "Attempting to start an increment before existing one is finalized"
		self.new_increment = Increment(self, int(self.containers_db["Increments"]))
		self.new_increment.start(base_index)
		self.increments.append(self.new_increment)
		self.containers_db["Increments"] = str(int(self.containers_db["Increments"])+1)

		message = self.new_increment.message()
		self.add_block(message,Digest.dataDigest(message),CODE_INCREMENT_START)
		return self.new_increment.index
	def finalize_increment(self,base_diff):
		if self.new_increment == None:
			raise "Attempting to finalized an increment but none was started"
		# TODO: Make the message include the same data as of the starting increment, so that
		#       they can be matched at recovery
		message = self.new_increment.message()
		self.add_block(message,Digest.dataDigest(message),CODE_INCREMENT_END)
		
		if len(self.containers)>0 and self.containers[-1] != None:
			self.containers[-1].finish_dump()
			self.save_container(self.containers[-1])
			self.new_increment.add_container(self.containers[-1].index)
			self.containers_db["Containers"] = str(len(self.containers))

		self.new_increment.finalize(base_diff)
		self.new_increment = None
	#
	# Utility methods for increment management
	#
	def last_finalized_increment(self):
		finalized_increments = [i for i in self.increments if i.finalized]
		if len(finalized_increments) == 0:
			return None
		return finalized_increments[-1].index
	def prev_increments(self):
		"""
		Compute the set of all the last increments, starting from
		the latest finalized one
		"""
		prev_increments = []
		for increment in self.increments:
			if increment.finalized:
				prev_increments = [increment.index]
			else:
				prev_increments.append(increment.index)
		return prev_increments
	def restore_increment(self, start_message, end_message, start_container, end_container, is_finalized):
		increment = Increment(self, int(self.containers_db["Increments"]))
		self.increments.append(self.new_increment)
		self.containers_db["Increments"] = str(int(self.containers_db["Increments"])+1)

		print "Restoring increment", start_message, end_message
		if start_message != end_message:
			is_finalized = False
		increment.restore(start_message, start_container, end_container, is_finalized)
	#
	# Reconstruction!
	#
	def reconstruct(self):
		#
		# It is the specific implementation of Storage that knows how to reconstruct the
		# containers
		#
		self.reconstruct_containers()
		#
		# TODO: Support cases where some of the containers are lost
		# or broken - in these case, do the best effort, i.e., recover
		# everything that is recoverable. In particular, in cases of redundancy,
		# when everything is recoverable, make sure we do it.
		#
		print "Scanning increments:"
		last_start_container = None
		last_start_message = None
		for container in self.containers:
			if container == None:
				continue
			print "Looking for increments in container", container.index
			start_message = container.find_increment_start()
			end_message = container.find_increment_end()
			if start_message != None:
				if last_start_container != None:
					# This is an unfinished increment
					# Create that increment, and start this one
					end_container = container.index-1
					self.restore_increment(last_start_message, end_message, last_start_container, end_container, False)
				if end_message == None:
					last_start_container = container.index
					last_start_message = start_message
			if end_message != None:
				# Found a finished increment
				if start_message != None:
					start_container = container.index
				elif last_start_container != None:
					start_container = last_start_container
					start_message = last_start_message
				else:
					print "Found increment end in container %d, but no previous increment start" % container.index
					continue
				# Create the increment
				end_container = container.index
				print "Found a finished increment in containers %d-%d" % (start_container,end_container)
				self.restore_increment(start_message, end_message, start_container, end_container, True)
				last_start_container = None
	#
	# Container management
	#
	def num_containers(self):
		return len(self.containers)
	def get_container(self,index):
		if self.containers[index] == None:
			self.containers[index] = self.load_container(index)
		return self.containers[index]
	def release_container(self,index):
		self.containers[index] = None
	def add_container(self):
		if len(self.containers)>0 and self.containers[-1] != None:
			if not self.containers[-1].frozen:
				# The previous container can belong to this increment or to the previous one.
				# In the second case, it did not start dumping and so does not need to be saved
				self.containers[-1].finish_dump()
				self.save_container(self.containers[-1])
			self.new_increment.add_container(self.containers[-1].index)
			self.containers_db["Containers"] = str(len(self.containers))
		container = Container(self.backup,len(self.containers))
		container.start_dump()
		self.containers.append(container)
		return container
	#
	# Block management
	#
	def add_block(self,data,digest,code):
		if len(self.containers)==0:
			container = self.add_container()
		else:
			container = self.containers[-1]
			if container==None or container.frozen or (not container.can_append(data)):
				container = self.add_container()
		index = container.append(data,digest,code)
		return (container.index, index)

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
	def load_container(self,index):
		print "Loading header for container", index, "     "
		container = Container(self.backup,index)
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.download(FileWriter(staging_path,self.down_bw_limiter), filename)
		container.load(staging_path)
		os.unlink(staging_path)
		return container
	
	def load_container_data(self,index):
		print "Loading data for container", index, "     "
		container = Container(self.backup,index)
		filename = container.filename()+".data"
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.download(FileWriter(staging_path,self.down_bw_limiter), filename)
		return staging_path
	
	def save_container(self,container):
		index = container.index
		
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		self.fs_handler.upload(FileReader(staging_path,self.up_bw_limiter), filename)
		self.fs_handler.upload(FileReader(staging_path+".data",self.up_bw_limiter), filename+".data")
		os.unlink(staging_path)
		os.unlink(staging_path+".data")
	def reconstruct_containers(self):
		print "Scanning containers:"
		container_files = {}
		container_data_files = {}
		file_list = self.fs_handler.list_files()
		print "listed files", file_list
		for file in file_list:
			container_index = self.backup.global_config.container_index(file,self.backup.label,"")
			if container_index != None:
				container_files[container_index] = file
			container_index = self.backup.global_config.container_index(file,self.backup.label,".data")
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
		print " Before loading we have %d containers " % self.num_containers()
		print "Loading %d containers:" % max_container
		for (index, file) in container_files.iteritems():
			if not container_data_files.has_key(index):
				print "Container", index, "has no data file :("
				continue
			container = self.load_container(index)
			self.containers[index] = container

class DirectoryStorage(Storage):
	"""
	Handler for a simple directory.
	"""
	def __init__(self):
		Storage.__init__(self)
		self.path = None
	def init(self,backup,txn_handler,params):
		Storage.init(self,backup,txn_handler)
		(path,) = params
		self.path = path
	def container_size(self):
		#return 4<<20
		return 4<<20
       	def reconstruct_containers(self):
		print "Scanning containers:", self.path
		container_files = {}
		container_data_files = {}
		for file in os.listdir(self.path):
			container_index = self.backup.global_config.container_index(file,self.backup.label,"")
			if container_index != None:
				container_files[container_index] = file
			container_index = self.backup.global_config.container_index(file,self.backup.label,".data")
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
	def load_container(self,index):
		print "Loading header for container", index, "     "
		container = Container(self.backup,index)

		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		container.load(os.path.join(self.path,filename))
		return container
	
	def load_container_data(self,index):
		print "Loading data for container", index, "     "
		container = Container(self.backup,index)

		filename = container.filename()+".data"
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
		target_path  = os.path.join(self.path, filename)
		return target_path
	def save_container(self,container):
		index = container.index
		
		filename = container.filename()
		staging_path = os.path.join(self.backup.global_config.staging_area(),filename)
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
		file.write("%s %s %s" % (account["user"],account["pass"],account["quota"]))
	def add_account(self,username,password,quota):
		self.accounts.append({"user":username, "pass":password, "quota":quota, "used":0})
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
		header_msg["Subject"] = "manent.%s.%s.header" % (self.backup.label, str(container.index))
		#header_msg["To"] = "gsasha@gmail.com"
		#header_msg["From"] = "gsasha@gmail.com"
		header_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		header_file = open(os.path.join(self.backup.global_config.staging_area(),filename), "rb")
		header_attch.set_payload(header_file.read())
		header_file.close()
		Encoders.encode_base64(header_attch)
		header_msg.attach(header_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com", "gsasha.manent1@gmail.com", header_msg.as_string())
		s.set_debuglevel(1)
		
		print "sending data in mail"
		data_msg = MIMEMultipart()
		data_msg["Subject"] = "manent.%s.%s.data" % (self.backup.label, str(container.index))
		data_attch = MIMEBase("application", "manent-container")
		filename = container.filename()
		data_file = open(os.path.join(self.backup.global_config.staging_area(),filename+".data"), "rb")
		data_attch.set_payload(data_file.read())
		data_file.close()
		Encoders.encode_base64(data_attch)
		data_msg.attach(data_attch)
		s.set_debuglevel(0)
		s.sendmail("gsasha.manent1@gmail.com", "gsasha.manent1@gmail.com", data_msg.as_string())
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

