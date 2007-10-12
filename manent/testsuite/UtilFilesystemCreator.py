import os, os.path, shutil
import stat

class FSCSymlink:
	def __init__(self,link):
		self.link = link
	def get_link(self):
		return self.link

class FilesystemCreator:
	def __init__(self,home):
		self.home = home
		try:
			shutil.rmtree(self.home)
		except:
			# If we run for the first time, the dir doesn't exists
			pass
		os.mkdir(self.home)

	def reset(self):
		try:
			shutil.rmtree(self.home)
		except:
			pass
		os.mkdir(self.home)
	def get_home(self):
		return self.home
	
	def add_files(self,files):
		self.__add_files(self.home,files)
	def __add_files(self,prefix,files):
		for name,contents in files.iteritems():
			path = os.path.join(prefix,name)
			if type(contents) == type({}):
				try:
					os.mkdir(path)
				except:
					pass
				self.__add_files(path,contents)
			elif isinstance(contents,FSCSymlink):
				try:
					os.symlink(contents.get_link(),path)
				except:
					pass
			else:
				f = open(path,"w")
				f.write(contents)
				f.close()
	def remove_files(self,files):
		self.__remove_files(self.home,files)
	def __remove_files(self,prefix,files):
		for name,contents in files.iteritems():
			path = os.path.join(prefix,name)
			if type(contents) == type({}):
				self.__add_files(path,contents)
				if len(os.listdir(path))==0:
					os.rmdir(path)
			else:
				os.unlink(path)
	def test_files(self,files):
		return not self.__test_files(self.home,files)
	def __test_files(self,prefix,files):
		failed = False
		for name,contents in files.iteritems():
			path = os.path.join(prefix,name)
			if type(contents) == type({}):
				try:
					result = os.lstat(path)
					if not stat.S_ISDIR(result[stat.ST_MODE]):
						failed = True
						print "***** expected to see directory in", path
						continue
					failed |= self.__test_files(path,contents)
				except:
					failed = True
					print "***** Could not read directory", path
			elif isinstance(contents,FSCSymlink):
				try:
					result = os.lstat(path)
					if not stat.S_ISLNK(result[stat.ST_MODE]):
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
	def link(self,file1,file2):
		if file1.startswith("/"):
			os.link(file1,os.path.join(self.home,file2))
		else:
			os.link(os.path.join(self.home,file1),os.path.join(self.home,file2))
	def test_link(self,file1,file2):
		stat1 = os.lstat(os.path.join(self.home,file1))
		stat2 = os.lstat(os.path.join(self.home,file2))
		return stat1[stat.ST_INO]==stat2[stat.ST_INO]
	def symlink(self,file1,file2):
		os.symlink(file1,os.path.join(self.home,file2))
	def symlink_absolute(self,file1,file2):
		if file1.startswith("/"):
			os.symlink(file1,os.path.join(self.home,file2))
		else:
			os.symlink(os.path.join(self.home,file1),os.path.join(self.home,file2))
	def lstat(self,file):
		return os.lstat(os.path.join(self.home,file))
	def test_lstat(self,file,expected_stat):
		failed = False
		file_stat = os.lstat(os.path.join(self.home,file))
		for idx in [stat.ST_MODE, stat.ST_UID, stat.ST_GID, stat.ST_SIZE, stat.ST_ATIME, stat.ST_MTIME]:
			failed |= file_stat[idx] != expected_stat[idx]
			if file_stat[idx] != expected_stat[idx]:
				print 'Mismatch in idx=%d: %d != %d' %(idx, file_stat[idx], expected_stat[idx])
		return not failed
	def chmod(self,file,mod):
		os.chmod(os.path.join(self.home,file),mod)
