#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import FileIO
import ftplib
import logging
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
      logging.debug("calling %s with params %s ..." % (
        message, str(args) + str(kwargs)))
      start = time.time()
      for i in range(retries):
        try:
          self.connect()
          result = func(self, *args, **kwargs)
          logging.debug("%2.3f seconds" % (time.time() - start))
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
    self.progress_reporter = None
  def set_progress_reporter(self, reporter):
    self.progress_reporter = reporter
  def list_files(self):
    pass
  def upload(self, file, remote_name):
    pass
  def download(self, file, remote_name):
    pass

class FTPHandler(RemoteFSHandler):
  def __init__(self, host, username, password, pkey_file, path):
    self.host = host
    self.path = path
    self.username = username
    self.password = password
    # For ftp, the pkey_file has no actual meaning.
    self.pkey_file = pkey_file
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
  def __init__(self, host, username, password, pkey_file, path):
    RemoteFSHandler.__init__(self)
    self.host = host
    #self.path = path
    self.path = path.replace("\\", "/")
    self.username = username
    self.password = password
    self.pkey_file = pkey_file
    # Internal data
    self.channel = None
    self.transport = None
  
  @retry_decorator(10, "list")
  def list_files(self):
    result = self.channel.listdir(self.path)
    self.cleanup_connection()
    return result
  
  @retry_decorator(10, "upload")
  def upload(self, file, remote_name):
    #print "Dummy uploading %s" % remote_name
    #return
    remote_path = os.path.join(self.path, remote_name)
    remote_path = remote_path.replace("\\", "/")
    handle = self.channel.file(remote_path, "wb")
    uploaded = 0
    for block in FileIO.read_blocks(file, 128<<10):
      uploaded += len(block)
      logging.debug("Uploaded %d" % uploaded)
      if self.progress_reporter is not None:
        self.progress_reporter.set(uploaded)
      handle.write(block)
    handle.close()
    self.cleanup_connection()
    
  @retry_decorator(10, "download")
  def download(self, file, remote_name):
    remote_path = os.path.join(self.path, remote_name)
    remote_path = remote_path.replace("\\", "/")
    handle = self.channel.file(remote_path, "rb")
    downloaded = 0
    for block in FileIO.read_blocks(handle, 16<<10):
      downloaded += len(block)
      logging.debug("Downloaded %d" % downloaded)
      if self.progress_reporter is not None:
        self.progress_reporter.set(downloaded)
      file.write(block)
    handle.close()
    self.cleanup_connection()
  
  @retry_decorator(10, "rename")
  def rename(self, old_name, new_name):
    old_path = os.path.join(self.path, old_name)
    old_path = old_path.replace("\\", "/")
    new_path = os.path.join(self.path, new_name)
    new_path = new_path.replace("\\", "/")
    self.channel.rename(old_path, new_path)
    self.cleanup_connection()
  
  @retry_decorator(10, "chmod")
  def chmod(self, remote_name, mode):
    remote_path = os.path.join(self.path, remote_name)
    remote_path = remote_path.replace("\\", "/")
    self.channel.chmod(remote_path, mode)
    self.cleanup_connection()
  #
  # Internal implementation
  #
  def connect(self):
    if self.channel is not None:
      return
    if self.pkey_file is not None:
      privatekeyfile = os.path.expanduser(self.pkey_file)
      mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
    else:
      mykey = None
    self.transport = paramiko.Transport((self.host, 22))
    self.transport.connect(username=self.username, password=self.password,
        pkey=mykey)
    self.channel = paramiko.SFTPClient.from_transport(self.transport)
  def cleanup_connection(self):
    if self.channel is not None:
      self.channel.close()
    if self.transport is not None:
      self.transport.close()
    self.transport = None
    self.channel = None
