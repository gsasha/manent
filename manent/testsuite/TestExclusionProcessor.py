import unittest

import manent.ExclusionProcessor as ExclusionProcessor
import testsuite.UtilFilesystemCreator as UtilFilesystemCreator

class EPDriver:
	"""Exclusion Processor Driver.
	   Runs the exclusion processor over the filesysem and collects the result
	   in the format of UtilFilesystemCreator"""
	def __init__(self, exclusion_processor, path):
		self.exclusion_processor = exclusion_processor
		self.path = path
	
	def check(self, expected_filesystem):
		filtered_filesystem = self.check_rec(self.exclusion_processor, self.path)
		return expected_filesystem == filtered_filesystem
	
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
		fsc = UtilFilesystemCreator()
		fsc.add_files(filesystem)
		
		ep = ExclusionProcessor.ExclusionProcessor()
		ep.add_wildcard_rule(ExclusionProcessor.RULE_EXCLUDE, "*.txt")
		filtered_files = ep.filter_files()

		expected_files = {"a":{"b":""}, "kuku.txt.bak": ""}
		self.ASSERT_EQUAL(filtered_files, expected_files)