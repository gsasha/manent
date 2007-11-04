import os, os.path
import fnmatch

RULE_EXCLUDE = 0
RULE_INCLUDE = 1

class FileListFilter:
	"""Filters a given list of files with a given wildcard pattern"""
	def __init__(self, action, pattern):
		self.action = action
		self.pattern = pattern
	
	def apply(self, in_files, ex_files):
		#print "Filtering ", in_files, ":", ex_files
		if self.action == RULE_EXCLUDE:
			new_in_files = []
			new_ex_files = [n for n in ex_files]
			for file in in_files:
				if fnmatch.fnmatch(file, self.pattern):
					new_ex_files.append(file)
				else:
					new_in_files.append(file)
			#print "  Rule=exclude, pattern=", self.pattern
			#print "  Result: ", new_in_files, ":", new_ex_files
			return (new_in_files, new_ex_files)
		else:
			new_in_files = [n for n in in_files]
			new_ex_files = []
			for file in ex_files:
				if fnmatch.fnmatch(file, self.pattern):
					new_in_files.append(file)
				else:
					new_ex_files.append(file)
			#print "  Rule=include, pattern=", self.pattern
			#print "  Result: ", new_in_files, ":", new_ex_files
			return (new_in_files, new_ex_files)
	
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
		self.rules = []
		self.wildcard_rules = []

	def add_rule(self, action, pattern):
		"""Adds a new rule to the database of rules at this level.
		The rule represents a relative path and applies only to subdirs
		that are below this path."""
		if os.path.isabs(pattern):
			raise Exception("Regular rules must be relative")
		
		steps = pattern.split(os.path.sep)
		if len(steps) == 0:
			raise Exception("Empty pattern rule")
		
		self.rules.append((action, steps))
	
	def add_absolute_rule(self, action, pattern):
		pattern = os.path.expanduser(pattern)
		assert os.path.isabs(pattern)
		
		root_steps = self.root.split(os.path.sep)
		patt_steps = pattern.split(os.path.sep)
		
		while True:
			if len(patt_steps) == 0:
				# The pattern does not reach to the root
				return
			if len(root_steps) == 0:
				self.add_rule(action, os.path.join(*patt_steps))
				return
			if not fnmatch.fnmatch(root_steps[0], patt_steps[0]):
				# The pattern does not apply to this path
				return
			
			root_steps = root_steps[1:]
			patt_steps = patt_steps[1:]

	def add_wildcard_rule(self, action, pattern):
		(dir_pattern, file_pattern) = os.path.split(pattern)
		if dir_pattern != "":
			raise Exception("Global rule must be either an absolute path or a file wildcard")
		self.wildcard_rules.append((action, file_pattern))
		
	def filter_files(self):
		"""Returns the list of files that pass filtering
		   in the current directory"""
		self.scan_local_excludes()

		#print "Filtering in %s:" % self.root
		#print "  rules", self.rules
		#print " wrules", self.wildcard_rules

		# We process files and directories differently.
		final_rules = []
		middle_rules = []
		for action, patterns in self.rules:
			if len(patterns) == 1:
				final_rules.append((action, patterns[0]))
			else:
				middle_rules.append((action, patterns))
		final_rules += self.wildcard_rules

		in_files = []
		in_dirs = []
		for file in os.listdir(self.root):
			file_path = os.path.join(self.root, file)
			if os.path.isdir(file_path) and not os.path.islink(file_path):
				in_dirs.append(file)
			else:
				in_files.append(file)

		ex_files = []
		ex_dirs = []
		for (action, pattern) in final_rules:
			filter = FileListFilter(action, pattern)
			in_files, ex_files = filter.apply(in_files, ex_files)
			in_dirs, ex_dirs = filter.apply(in_dirs, ex_dirs)

		self.included_files = in_files
		self.included_dirs = in_dirs
		self.dir_rules = middle_rules
		
		#print "Result of filtering in %s:" % self.root
		#print "  files", self.included_files
		#print "  dirs ", self.included_dirs
		#print "  rules", self.dir_rules

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
		ep = ExclusionProcessor(os.path.join(self.root, directory))

		for (action, patterns) in self.dir_rules:
			if fnmatch.fnmatch(directory, patterns[0]):
				ep.add_rule(action, os.path.join(*patterns[1:]))
		for (action, pattern) in self.wildcard_rules:
			ep.add_wildcard_rule(action, pattern)

		return ep
	def __scan(self):
		"""Private implementation.
		Scans the current directory for additional include and exclude
		directives"""
		pass
	
	def scan_local_excludes(self):
		"""
		Local exclude files contain the following lines:
			#<text> : comment
			exclude=<pattern>
			include=<pattern>
		Include and exclude patterns can be only relative and wildcards
		No absolute patterns are accepted (and why should they be in local
		rules?)
		
		The reason to disallow absolute rules is that they can cause
		some very complex interactions. For example:
		"""
		try:
			exclude_file_name = os.path.join(self.root, ".manent-exclude")
			#print "Trying to find local exclude in", exclude_file_name
			file = open(exclude_file_name)
		except:
			# If the file does not exist or can't be opened, ignore it
			return
		
		for line in file:
			line = line.strip()
			if line.startswith("#"):
				# Nothing to do for a comment line
				continue
			elif line.startswith("include="):
				action = RULE_INCLUDE
				pattern = line[len("include="):]
			elif line.startswith("exclude="):
				action = RULE_EXCLUDE
				pattern = line[len("exclude="):]
			else:
				print "Warning: unrecognized rule line in file %s: %s" % (
					exclude_file_name, line)
				continue
			dir_pat, file_patt = os.path.split(pattern)
			if dir_pat == "":
				self.add_wildcard_rule(action, pattern)
			else:
				self.add_rule(action, pattern)

