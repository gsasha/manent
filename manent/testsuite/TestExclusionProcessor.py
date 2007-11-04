import unittest
import os, os.path

import manent.ExclusionProcessor as EP
import UtilFilesystemCreator as FSC

class EPDriver:
	"""Exclusion Processor Driver.
	   Runs the exclusion processor over the filesysem and collects the result
	   in the format of UtilFilesystemCreator"""
	def __init__(self, exclusion_processor, path):
		self.exclusion_processor = exclusion_processor
		self.path = path
	
	def check(self, expected_fs):
		filtered_fs = self.check_rec(self.exclusion_processor, self.path)
		if expected_fs != filtered_fs:
			print "Expected fs: ", expected_fs
			print "Filtered fs: ", filtered_fs
		return expected_fs == filtered_fs
	
	def check_rec(self, ep, path):
		fs = {}
		
		ep.filter_files()
		filtered_files = ep.get_included_files()
		filtered_dirs = ep.get_included_dirs()

		for name in filtered_files:
			new_path = os.path.join(path, name)
			assert not os.path.isdir(new_path) or os.path.islink(new_path)
			fs[name] = ""
		for name in filtered_dirs:
			new_path = os.path.join(path, name)
			assert os.path.isdir(new_path)
			new_ep = ep.descend(name)
			fs[name] = self.check_rec(new_ep, new_path)

		return fs

class TestExclusionProcessor(unittest.TestCase):

	def setUp(self):
		self.fsc = FSC.FilesystemCreator()
	def tearDown(self):
		self.fsc.cleanup()
		
	def test_wildcard_pattern(self):
		filesystem = {"a":{"b":""}, "kuku.txt":"", "kuku.txt.bak": ""}
		self.fsc.reset()
		self.fsc.add_files(filesystem)
		
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		ep.add_wildcard_rule(EP.RULE_EXCLUDE, 'a')
		ep.add_wildcard_rule(EP.RULE_EXCLUDE, '*.txt')
		
		expected_files = {"kuku.txt.bak": ""}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_files))

	def test_deep_pattern(self):
		"""Test that a pattern with several wildcards in it works"""
		filesystem = {"a":{"b":""},"c":{"d.txt":"","e.cc":""}}
		self.fsc.reset()
		self.fsc.add_files(filesystem)
		
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		ep.add_rule(EP.RULE_EXCLUDE, "*/*.txt")
		
		expected_fs = {"a":{"b":""}, "c":{"e.cc":""}}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))

	def test_rule_override(self):
		"""Test that a include rule overrides an exclude one, and
		   vice versa"""
		filesystem = {"a":{"b.txt":"", "b.bak":"","c.txt":"","d.txt":""}}
		self.fsc.reset()
		self.fsc.add_files(filesystem)
		
		# Test that inclusion overrides exclusion
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		ep.add_rule(EP.RULE_EXCLUDE, "*/*.txt")
		ep.add_rule(EP.RULE_INCLUDE, "a/b*")
		
		expected_fs = {"a":{"b.txt":"", "b.bak":""}}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))
		
		# Test that exclusion overrides inclusion
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		# - Note the rule order: the wildcard rules are executed last
		#   although they can be defined earlier
		ep.add_wildcard_rule(EP.RULE_EXCLUDE, "*.bak")
		ep.add_rule(EP.RULE_EXCLUDE, "*/*")
		ep.add_rule(EP.RULE_INCLUDE, "a/b*")

		expected_fs = {"a":{"b.txt":""}}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))

	def test_rule_absolute(self):
		"""Test that absolute path rules work"""
		filesystem = {"a":{"b.txt":""},".manent":{"config":"a", "db":"b"}}
		self.fsc.reset()
		self.fsc.add_files(filesystem)
		
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		ep.add_absolute_rule(EP.RULE_EXCLUDE, os.path.join(
			self.fsc.get_home(),".manent"))
			
		expected_fs = {"a":{"b.txt":""}}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))

	def test_rule_local(self):
		"""Test that local rules defined in the filesystem work"""
		self.fsc.reset()
		filesystem = {"a":{"b.txt":"", "c.txt":"", "d.txt":""}}
		self.fsc.add_files(filesystem)
		self.fsc.add_files(
			{".manent-exclude":"exclude=b.txt\ninclude=a/d*"})
		
		ep = EP.ExclusionProcessor(self.fsc.get_home())
			
		expected_fs = {"a":{"c.txt":"", "d.txt":""}, ".manent-exclude":""}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))

	# TODO: test that a symlink to a directory is not considered directory
	#       for purposes of exclusion
	def test_symlink_to_dir(self):
		self.fsc.reset()
		f2 = FSC.FSCSymlink("d")
		fs = {"d":{"a":"", "b":""}, "f":f2}
		self.fsc.add_files(fs)
		
		ep = EP.ExclusionProcessor(self.fsc.get_home())
		ep.add_rule(EP.RULE_EXCLUDE, "d/a")
		ep.add_rule(EP.RULE_EXCLUDE, "f/b")
		
		expected_fs = {"d":{"b":""}, "f":""}
		driver = EPDriver(ep, self.fsc.get_home())
		self.failUnless(driver.check(expected_fs))