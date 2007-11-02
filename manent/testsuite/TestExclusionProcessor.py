import unittest

import manent.ExclusionProcessor as EP
import testsuite.UtilFilesystemCreator as FSC

class EPDriver:
	"""Exclusion Processor Driver.
	   Runs the exclusion processor over the filesysem and collects the result
	   in the format of UtilFilesystemCreator"""
	def __init__(self, exclusion_processor, path):
		self.exclusion_processor = exclusion_processor
		self.path = path
	
	def check(self, expected_fs):
		filtered_fs = self.check_rec(self.exclusion_processor, self.path)
		return expected_fs == filtered_fs
	
	def check_rec(self, ep, path):
		fs = {}
		
		filtered_files = ep.filter_files()
		for file in filtered_files:
			new_path = os.path.join(path, file)
			if os.isdir(new_path):
				new_ep = ep.descend(file)
				fs[file] = self.check_rec(new_ep,new_path)
			else:
				fs[file] = {}
		return fs

class TestExclusionProcessor(unittest.TestCase):

	def test_wildcard_pattern(self):
		filesystem = {"a":{"b":""}, "kuku.txt":"", "kuku.txt.bak": ""}
		fsc = FSC.FilesystemCreator("/tmp/manent.test.scratch.exclusion")
		fsc.add_files(filesystem)
		
		ep = EP.ExclusionProcessor(fsc.get_home())
		ep.add_wildcard_rule('*.txt', EP.RULE_EXCLUDE)
		
		expected_files = {"a":{"b":""}, "kuku.txt.bak": ""}
		driver = EPDriver(ep, fsc.get_home())
		self.failUnless(driver.check(expected_files))

