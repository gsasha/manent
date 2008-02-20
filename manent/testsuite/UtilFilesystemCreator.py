#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import os, os.path, shutil
import stat
import tempfile

import manent.Config as Config

class FSCSymlink:
	def __init__(self, link):
		self.link = link
	def get_link(self):
		return self.link

class FSCFile:
	def __init__(self, data):
		self.data = data
	def get_data(self):
		return self.data

def supports_hard_links():
	if os.name == 'Darwin':
		return True
	elif os.name == 'posix':
		return True
	elif os.name == 'nt':
		return False
	raise Exception("Unsupported OS")

def supports_symbolic_links():
	if os.name == 'Darwin':
		return True
	elif os.name == 'posix':
		return True
	elif os.name == 'nt':
		return False
	raise Exception("Unsupported OS")

class FilesystemCreator:
	def __init__(self):
		self.home = tempfile.mkdtemp("", "manent.test.scratch",
			Config.paths.temp_area())
		#print "*** Selected homedir %s " % self.home
		try:
			shutil.rmtree(self.home)
		except:
			# If we run for the first time, the dir doesn't exists
			pass
		os.mkdir(self.home, 0700)

	def cleanup(self):
		try:
			shutil.rmtree(self.home)
		except:
			pass

	def reset(self):
		self.cleanup()
		os.mkdir(self.home, 0700)

	def get_home(self):
		return self.home
	
	def add_files(self, files):
		self.hlink_dict = {}
		self.__add_files(self.home, files)
	def __add_files(self, prefix, files):
		for name, contents in files.iteritems():
			path = os.path.join(prefix, name)
			#
			# OK, it's no hardlink. Create it.
			#
			if type(contents) == type({}):
				try:
					os.mkdir(path)
				except:
					pass
				self.__add_files(path, contents)
				continue

			#
			# Check if a hard link is due
			#
			if self.hlink_dict.has_key(contents):
				os.link(self.hlink_dict[contents], path)
				continue
			if type(contents) != type(""):
				# Strings might be re-used by python, so we allow
				# hard link recognition only for files that are explicitly
				# made as FSCFile
				self.hlink_dict[contents] = path
				
			if isinstance(contents, FSCSymlink):
				try:
					os.symlink(contents.get_link(), path)
				except:
					pass
			elif isinstance(contents, FSCFile):
				try:
					f = open(path,"w")
					f.write(contents.get_data())
					f.close()
				except:
					pass
			else:
				f = open(path,"w")
				f.write(contents)
				f.close()
	def remove_files(self, files):
		self.__remove_files(self.home, files)
	def __remove_files(self, prefix, files):
		for name, contents in files.iteritems():
			path = os.path.join(prefix, name)
			if type(contents) == type({}):
				self.__add_files(path, contents)
				if len(os.listdir(path))==0:
					os.rmdir(path)
			else:
				os.unlink(path)
	def test_files(self, files):
		self.hlink_dict = {}
		return not self.__test_files(self.home, files)
	def __test_files(self, prefix, files):
		failed = False
		for name, contents in files.iteritems():
			path = os.path.join(prefix, name)
			st = os.lstat(path)

			#
			# Recurse in for a directory
			#
			if type(contents) == type({}):
				try:
					if not stat.S_ISDIR(st[stat.ST_MODE]):
						failed = True
						print "***** expected to see directory in", path
						continue
					failed |= self.__test_files(path,contents)
				except:
					failed = True
					print "***** Could not read directory", path
				continue
				
			#
			# It's a hard link. Test that it connects to the right inode
			#
			if os.name == 'Darwin' and isinstance(contents, FSCSymlink):
				# On MacOS, it appears that it is not possible to add a hard link
				# to a symlink (is the symlink not an inode there?).
				# Therefore, we do not check symbolic links in the hlinks dictionary
				if self.hlink_dict.has_key(contents):
					# It's supposed to be a hard link to a symlink,
					# but since it's not gonna happen on Darwin, this case is OK
					continue
				else:
					self.hlink_dict[contents] = st[stat.ST_INO]
			elif self.hlink_dict.has_key(contents):
				if st[stat.ST_INO] != self.hlink_dict[contents]:
					failed = True
					print "***** expected %s to be a hard link" % path
					continue
			else:
				self.hlink_dict[contents] = st[stat.ST_INO]
			
			if isinstance(contents, FSCSymlink):
				try:
					if not stat.S_ISLNK(st[stat.ST_MODE]):
						failed = True
						print "***** expected to see symlink in", path
						continue
					link = os.readlink(path)
					if os.readlink(path) != contents.get_link():
						failed = True
						print "***** Link contents incorrect", path, "expected:", contents.get_link(), "got:", os.readlink(path)
						continue
				except:
					failed = True
					print "***** Could not read symlink", path
			elif isinstance(contents, FSCFile):
				try:
					file = open(path, "r")
					if file.read() != contents.get_data():
						failed = True
						print "***** Mismatching contents reading file", path
				except:
					failed = True
					print "***** Could not read file", path
			else:
				try:
					file = open(path, "r")
					if file.read() != contents:
						failed = True
						print "***** Mismatching contents reading file", path
				except:
					failed = True
					print "***** Could not read file", path
		return failed
	def link(self, file1, file2):
		if file1.startswith("/"):
			os.link(file1, os.path.join(self.home, file2))
		else:
			os.link(os.path.join(self.home, file1),
			        os.path.join(self.home, file2))
	def test_link(self, file1, file2):
		stat1 = os.lstat(os.path.join(self.home, file1))
		stat2 = os.lstat(os.path.join(self.home, file2))
		return stat1[stat.ST_INO]==stat2[stat.ST_INO]
	def symlink(self, file1, file2):
		os.symlink(file1, os.path.join(self.home, file2))
	def symlink_absolute(self, file1, file2):
		if file1.startswith("/"):
			os.symlink(file1, os.path.join(self.home, file2))
		else:
			os.symlink(os.path.join(self.home, file1),
			           os.path.join(self.home, file2))
	def lstat(self, file):
		return os.lstat(os.path.join(self.home, file))
	def test_lstat(self,file,expected_stat):
		failed = False
		file_stat = os.lstat(os.path.join(self.home, file))
		for idx in [stat.ST_MODE, stat.ST_UID, stat.ST_GID,
		            stat.ST_SIZE, stat.ST_ATIME, stat.ST_MTIME]:
			failed |= file_stat[idx] != expected_stat[idx]
			if file_stat[idx] != expected_stat[idx]:
				print 'Mismatch in idx=%d: %d != %d' % (
				    idx, file_stat[idx], expected_stat[idx])
		return not failed
	def chmod(self, file, mod):
		os.chmod(os.path.join(self.home, file), mod)
