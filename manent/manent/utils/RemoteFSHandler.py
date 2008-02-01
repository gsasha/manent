import FileIO
import ftplib
import paramiko
import os, os.path
import time
import traceback

#----------------------------------------------------
# Decorator that implements retrying
#----------------------------------------------------
def retry_decorator(retries, message):
	def impl(func):
		def retrier(self, *args, **kwargs):
			print "calling", message, "with params", args, kwargs, "...",
			start = time.time()
			for i in range(retries):
				try:
					self.connect()
					result = func(self, *args, **kwargs)
					print "", time.time() - start, "seconds"
					return result
				except:
					traceback.print_exc()
					self.cleanup_connection()
			else:
				raise "Failed to %s for %d times. Giving up" % (message,
				                                                retries)
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
	def upload(self, file, remote_name):
		pass
	def download(self, file, remote_name):
		pass

class FTPHandler(RemoteFSHandler):
	def __init__(self, host, username, password, path):
		self.host = host
		self.path = path
		self.username = username
		self.password = password
		# Internal data
		self.ftp = None
	
	@retry_decorator(10, "list files")
	def list_files(self):
		return self.ftp.nlst()
	
	@retry_decorator(10, "upload")
	def upload(self, file, remote_name):
		self.ftp.storbinary("STOR %s" % (remote_name), file)
	
	@retry_decorator(10, "download")
	def download(self, file, remote_name):
		self.ftp.retrbinary("RETR %s" % (remote_name), file.write, 100<<10)

	@retry_decorator(10, "rename")
	def rename(self, old_name, new_name):
		self.ftp.rename(old_name, new_name)

	@retry_decorator(10, "chmod")
	def chmod(self, file_name, mode):
		# Python's FTP doesn't know how to do this
		pass
	# --------
	# Internal implementation
	# --------
	def connect(self):
		if self.ftp != None:
			return
		print "Connecting to %s as %s" % (self.host, self.username)
		self.ftp = ftplib.FTP(self.host, self.username, self.password)
		self.ftp.set_pasv(False)
		self.ftp.cwd(self.path)
		print "Changing dir to", self.path
	def cleanup_connection(self):
		self.ftp = None

class SFTPHandler(RemoteFSHandler):
	def __init__(self, host, username, password, path):
		RemoteFSHandler.__init__(self)
		self.host = host
		#self.path = path
		self.path = path.replace("\\", "/")
		self.username = username
		self.password = password
		# Internal data
		self.channel = None
		self.transport = None
	
	@retry_decorator(10, "list")
	def list_files(self):
		return self.channel.listdir(self.path)
	
	@retry_decorator(10, "upload")
	def upload(self, file, remote_name):
		#print "Dummy uploading %s" % remote_name
		#return
		remote_path = os.path.join(self.path, remote_name)
		remote_path = remote_path.replace("\\", "/")
		handle = self.channel.file(remote_path, "wb")
		for block in FileIO.read_blocks(file, 128<<10):
			handle.write(block)
		handle.close()
		
	@retry_decorator(10, "download")
	def download(self, file, remote_name):
		remote_path = os.path.join(self.path, remote_name)
		remote_path = remote_path.replace("\\", "/")
		handle = self.channel.file(remote_path, "rb")
		for block in FileIO.read_blocks(handle, 16<<10):
			file.write(block)
		handle.close()
	
	@retry_decorator(10, "rename")
	def rename(self, old_name, new_name):
		old_path = os.path.join(self.path, old_name)
		old_path = old_path.replace("\\", "/")
		new_path = os.path.join(self.path, new_name)
		new_path = new_path.replace("\\", "/")
		self.channel.rename(old_path, new_path)
	
	@retry_decorator(10, "chmod")
	def chmod(self, remote_name, mode):
		remote_path = os.path.join(self.path, remote_name)
		remote_path = remote_path.replace("\\", "/")
		self.channel.chmod(remote_path, mode)
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
