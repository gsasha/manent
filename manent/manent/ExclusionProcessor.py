import os, os.path
import fnmatch

RULE_EXCLUDE = 0
RULE_INCLUDE = 1

class FileListFilter:
	"""Filters a given list of files with a given wildcard pattern"""
	def __init__(self, pattern, rule):
		self.rule = rule
		self.pattern = pattern
	
	def apply(self, in_files, ex_files):
		if self.rule == RULE_EXCLUDE:
			new_in_files = []
			new_ex_files = [n for n in ex_files]
			for file in in_files:
				if fnmatch.fnmatch(file,pattern):
					new_ex_files.append(file)
				else:
					new_in_files.append(file)
			return (new_in_files,new_ex_files)
		else:
			new_in_files = [n for n in in_files]
			new_ex_files = []
			for file in in_files:
				if fnmatch.fnmatch(file,pattern):
					new_in_files.append(file)
				else:
					new_ex_files.append(file)
			return (new_in_files,new_ex_files)
	
class ExclusionProcessor:
	def __init__(self, root):
		# Each rule is a tuple of:
		# 1. Specification whether the rule is an exclude or an include one
		# 2. list of patterns corresponding to each of the path elements
		
		# Relative rules are defined with respect to the root of the
		# FileFilterEngine
		# Absolute rules are defined with respect to the root of the filesystem
		# Wildcard rules are simple patterns without a directory path.
		self.root = root
		self.relative_rules = []
		self.wildcard_rules = []

	def add_rule(self, pattern, action):
		"""Adds a new rule to the database of rules at this level.
		The rule represents a relative path and applies only to subdirs
		that are below this path."""
		if os.path.isabs(pattern):
			raise Exception("Regular rules must be relative")
		
		steps = pattern.split(os.path.sep)
		if len(steps) == 0:
			raise Exception("Empty pattern rule")
		
		self.rules.append((action, steps))
	
	def add_absolute_rule(self, pattern, action):
		patern = os.path.expanduser(pattern)
		assert os.path.isabs(patern)
		
		root_steps = self.root.split(os.path.sep)
		patt_steps = pattern.split(os.path.sep)
		
		while True:
			if len(patt_steps) == 0:
				# The pattern does not reach to the root
				return
			if len(root_steps) == 0:
				self.add_rule(os.path.join(patt_steps), action)
				return
			if not fnmatch.fnmatch(root_steps[0], pat_steps[0]):
				# The pattern does not apply to this path
				return
			
			root_steps = root_steps[1:]
			patt_steps = patt_steps[1:]

	def add_wildcard_rule(self, pattern, action):
		(dir_pattern, file_pattern) = os.path.split(pattern)
		if dir_pattern != "":
			raise Exception("Global rule must be either an absolute path or a file wildcard")
		self.wildcard_rules.append((action, file_pattern))
		
	def filter_files(self):
		"""Returns the list of files that pass filtering
		   in the current directory"""
		# We process files and directories differently.
		final_rules = []
		middle_rules = []
		for action, patterns in self.rules:
			if len(patterns) == 1:
				final_rules.append((action, patterns[0]))
			else:
				middle_rules.append((action, patterns))

		in_files = []
		in_dirs = []
		for file in os.listdir(self.root):
			file_path = os.path.join(self.root, file)
			if os.path.isdir(file_path):
				in_dirs.append(file)
			else:
				in_files.append(file)

		ex_files = []
		ex_dirs = []
		for (action, pattern) in final_rules:
			filter = FileListFilter(action, pattern)
			in_files, ex_files = filter.apply(in_files, ex_files)
			inc_dirs, ex_dirs = filter.apply(in_dirs, ex_dirs)
			
		self.included_files = in_files
		self.included_dirs = in_dirs
		self.dir_rules = middle_rules

	def get_included_files(self):
		return self.included_files
	def get_included_dirs(self):
		return self.included_dirs
		
	def descend(self,directory):
		"""Returns a new filter that can process files
		   in the given subdirectory.
		   The directory that is given as a parameter must be
		   a valid subdir of this one"""
		assert directory in self.included_dirs
		fe = FileFilterEngine()

		for (action, patterns) in self.dir_rules:
			if fnmatch.fnmatch(directory, patterns[0]):
				fe.add_rule((action, patterns[1:]))
		for (action, patterns) in self.wildcard_rules:
			fe.add_wildcard_rule((action, patterns))

		return fe
	def __scan(self):
		"""Private implementation.
		Scans the current directory for additional include and exclude
		directives"""
		pass
	
	def scan_local_exclusion(self):
		try:
			file = open(os.path.join(self.path, ".manent-excludes"))
		except:
			# If the file does not exist or can't be opened, ignore it
			return
		
		for line in file:
			if line.startswith():
				pass
			
