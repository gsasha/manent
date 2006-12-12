from __future__ import with_statement

import os
import random
import cStringIO

class GeneratedFile:
	def __init__(self, depth, parent):
		self.depth = depth
		self.parent = parent
		self.contentsSeed = os.urandom(8)
		self.RandomizeSize()
		self.RandomizeName()
		self.RandomizeAttrs()
		
	def RandomizeSize(self):
		sizeType = random.randint(0, 4)
		if sizeType == 0:   # empty file
			self.size = 0
		elif sizeType == 1: # small file
			self.size = random.randint(0, 512 - 1)
		elif sizeType == 2: # a bit larger file
			self.size = random.randint(512, 100 * 1024)
		elif sizeType == 3: # very large file
			inv = random.uniform(0, 0000001)
			if inv != 0:
				self.size = int(1./inv)
			else:
				self.size = 1024 * 1024
		else:
			self.size = random.randint(1024, int(1024*1024 / self.depth))
		
	def RandomizeName(self):
		self.name = ""
		for i in xrange(1, 10):
			self.name = self.name + chr(random.randint(ord('a'), ord('z')))
			
		self.path = os.path.join(self.parent.path, self.name)

	def RandomizeAttrs(self):
		pass
		
	def DigestContents(self):
		pass

	def CreateAtFileSystem(self, where):
		real_path = os.path.join(where, self.path)
		file = open(real_path, "wb")
		bytesleft = self.size
		while bytesleft > 0:
			blockSize = min([bytesleft, 1024*1024])
#			output = cStringIO.StringIO()
#			for i in xrange(0, blockSize):
#				output.write(chr(int(random.random() * 256)))
#			file.write(output.getvalue())
			strdata = ''.join([chr(int(random.random() * 256)) for num in xrange(blockSize)])
			file.write(strdata)
			bytesleft = bytesleft - blockSize
		file.close()

	def RemoveFromFileSystem(self, where):
		os.remove(os.path.join(where, self.path))
		
	def totalSize(self):
		return self.size
	
	def printSelf(self, indent):
		print (' ' * indent) + "Name: " + self.name + ", size: " + str(self.size)


class GeneratedDir:
	def __init__(self, depth, parent):
		self.depth = depth
		self.parent = parent
		self.children = {}
		self.RandomizeName()
		print "Creating dir " + self.name
		self.RandomizeFiles()
		if self.depth > 5:
			return

		self.RandomizeSubDirs()
		
	def RandomizeFiles(self):
		randVal = random.randint(0, 10)
		if randVal == 0:   # empty dir
			self.numFiles = 0
		elif randVal == 1: # small dir
			self.numFiles = random.randint(0, 10)
		elif randVal == 2: # a bit larger dir
			self.numFiles = random.randint(10, 50)
		elif randVal == 3 and random.randint(0, 50) == 7: # lots of files, very small chance
			self.numFiles = random.randint(20000, 50000)
		else:
			self.numFiles = random.randint(0, 1000)
			
		while len(self.children) < self.numFiles:
			f = GeneratedFile(self.depth + 1, self)
			if not self.children.has_key(f.name):
				self.children[f.name] = f

	def RandomizeSubDirs(self):
		randVal = random.randint(0, 20)
		if randVal < 3:   # empty dir
			self.numSubDirs = 0
		elif randVal == 11: # small dir
			self.numSubDirs = random.randint(0, 5)
		elif randVal == 12: # a bit larger dir
			self.numSubDirs = random.randint(5, 50)
		else:
			self.numSubDirs = int(random.expovariate(2. ** self.depth))
		
		print "There will be " + str(self.numSubDirs) + " subdirs and " + \
				str(self.numFiles) + " files in " + self.name; 
			
		generatedDirs = 0
		while generatedDirs < self.numSubDirs:
			f = GeneratedDir(self.depth + 1, self)
			if not self.children.has_key(f.name):
				self.children[f.name] = f
				generatedDirs = generatedDirs +1

	def RandomizeName(self):
		self.name = ""
		for i in xrange(1, 10):
			self.name = self.name + chr(random.randint(ord('a'), ord('z')))
			
		if self.parent == None:
			self.path = self.name
		else:
			self.path = os.path.join(self.parent.path, self.name)
	
	def CreateAtFileSystem(self, where):
		real_path = os.path.join(where, self.path)
		os.mkdir(real_path)
		for i in self.children.values():
			i.CreateAtFileSystem(where)
	
	def RemoveFromFileSystem(self, where):
		real_path = os.path.join(where, self.path)
		for i in self.children.values():
			i.RemoveFromFileSystem(where)
		os.rmdir(real_path)
	
	def totalSize(self):
		sum = 0
		for i in self.children.values():
			sum += i.totalSize()
		return sum
	
	def printSelf(self, indent):
		print (' ' * indent) + "Directory " + self.path + ':'
		for i in self.children.values():
			i.printSelf(indent + 4)

class FileTree:
	def __init__(self):
		self.root = GeneratedDir(1, None)
	
	def printSelf(self):
		self.root.printSelf(0);
		
	def totalSize(self):
		return self.root.totalSize()
	
	def CreateAtFileSystem(self, path):
		self.root.CreateAtFileSystem(path)
		
	def RemoveFromFileSystem(self, path):
		self.root.RemoveFromFileSystem(path)

#
# Demo usage
# 
def main():	
	tree = FileTree()
	path = "c:\\temp\\manent\\source"
	totalSize = tree.totalSize()
	print "The total tree size is " + str(totalSize/(1024.*1024))  + " mbytes"
	
	if os.name == 'nt':
		import win32file
		freeData = win32file.GetDiskFreeSpace("c:\\")
		freespace = freeData[0] * freeData[1] * freeData[2]
	else:
		freespace = 0
		
	print "Free space on drive " + str(freespace/(1024.*1024))  + " mbytes"
	
	if freespace - totalSize - 1*1024*1024*1024: # leavw 1GB of free space
		tree.CreateAtFileSystem(path)
		s = raw_input("Press enter to delete tree")
		tree.RemoveFromFileSystem(path)
	else:
		print "Too little free space, not creating test tree"

if __name__ == "__main__":
    main()