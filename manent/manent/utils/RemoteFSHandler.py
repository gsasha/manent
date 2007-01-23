from FileIO import read_blocks
import os, os.path
import traceback
import paramiko
from ftplib import *

#----------------------------------------------------
# Decorator that implements retrying
#----------------------------------------------------
def retry_decorator(retries,message):
	def impl(func):
		def retrier(self,*args,**kwargs):
			for i in range(retries):
				try:
					self.connect()
					return func(self,*args,**kwargs)
				except:
					traceback.print_exc()
					self.cleanup_connection()
			else:
				raise "Failed to %s for %d times. Giving up" % (message,retries)
		return retrier
	return impl

#-----------------------------------------------------------
# Remote network access classes
#-----------------------------------------------------------
class RemoteFSHandler:
	"""
	This class defines the interface that all the remote FS handlers
	must implement.
	"""
	def __init__(self):
		pass
	def list_files(self):
		pass
	def upload(self,file,remote_name):
		pass
	def download(self,file,remote_name):
		pass

class FTPHandler(RemoteFSHandler):
	def __init__(self,host,username,password,path):
		self.host = host
		self.path = "."
		self.username = username
		self.password = password
		# Internal data
		self.ftp = None
	
	@retry_decorator(10, "list files")
	def list_files(self):
		return self.ftp.nlst()
	
	@retry_decorator(10, "upload")
	def upload(self,file,remote_name):
		self.ftp.storbinary("STOR %s" % (remote_name), FileReader(file))
	
	@retry_decorator(10, "download")
	def download(self,file,remote_name):
		self.ftp.retrbinary("RETR %s" % (filename), FileWriter(staging_path).write,100<<10)
	# --------
	# Internal implementation
	# --------
	def connect(self):
		if self.ftp != None:
			return
		print "Connecting to %s as %s" % (self.server,self.user)
		self.ftp = FTP(self.server,self.user,self.password)
		self.ftp.set_pasv(False)
		self.ftp.cwd(self.path)
	def cleanup_connection(self):
		self.ftp = None

class SFTPHandler(RemoteFSHandler):
	def __init__(self,host,username,password,path):
		RemoteFSHandler.__init__(self)
		self.host = host
		self.path = path
		self.username = username
		self.password = password
		# Internal data
		self.channel = None
		self.transport = None
	
	@retry_decorator(10, "list")
	def list_files(self):
		return self.channel.listdir(self.path)
	
	@retry_decorator(10, "upload")
	def upload(self,file,remote_name):
		handle = self.channel.file(os.path.join(self.path,remote_name), "w")
		for block in read_blocks(file, 4096):
			handle.write(block)
		handle.close()
		
	@retry_decorator(10, "download")
	def download(self,remote_name,file):
		handle = self.channel.file(os.path.join(self.path,remote_name), "r")
		for block in read_blocks(handle, 4096):
			file.write(block)
		handle.close()
	#
	# Internal implementation
	#
	def connect(self):
		if self.channel is not None:
			return
		self.transport = paramiko.Transport((self.host, 22))
		self.transport.connect(username=self.username, password=self.password)
		self.channel = paramiko.SFTPClient.from_transport(self.transport)
	def cleanup_connection(self):
		self.transport = None
		self.channel = None
