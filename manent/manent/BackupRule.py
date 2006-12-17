import os

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
			if path.startswith(i):
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