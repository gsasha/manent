import os

def isParent(parentPath, childPath):
	parentPath = os.path.normpath(parentPath)
	parentPath = os.path.normcase(parentPath)
	childPath = os.path.normpath(childPath)
	childPath = os.path.normcase(childPath)
	
	while True:
		if (len(parentPath) == len(childPath)):
			return parentPath == childPath
		if (len(childPath) < len(parentPath)):
			return False
		
		childPath = os.path.split(childPath)[0]

class BackupRule:
	def __init__(self):
		self.included = []
		self.excluded = []
		self.excludedFilePatterns = []
	
	def getFiles(self):
		pass
	
	def addIncludedTree(self, directory):
		self.included.append(directory)
	
	def addExcludedSubTree(self, directory):
		self.excluded.append(directory)
	
	def addExcludedFilePattern(self, pattern):
		self.excludedFilePatterns.append(pattern)
	
	def isIncluded(self, path):
		for i in self.included:
			if isParent(i, path):
				return True
		return False
		# check that it's not excluded.
	
	def setBackupFrequecy(self, months, days, hours, minutes):
		pass

# test
#rule= BackupRule()
#rule.addIncludedTree("c:\\windows\\system32")
#rule.addIncludedTree("c:\\windows\\system")
#print rule.isIncluded("c:\\windows\\system\\a.txt")

#print os.path.normcase("c:\\\\\\fIle\\.\\windows\\\\\\")
#print isParent("c:/file2/windows2", "c:\\file2\\windows2\\fasa");
#print isParent("c:/", "d:\\");
#print isParent("\\\\genadyxp\\c$\\windows", "\\\\genadyxp\\c$\\file2fjksahfjkdsahfkjdshmvc,z");