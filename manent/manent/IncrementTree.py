
#
# String constants for the tree node formatting
#
TREE_NEXT_INCREMENT      = "tree.next_increment"
TREE_FIRST_UNFINALIZED   = "tree.first_unfinalized_increment"
TREE_CHILD_IDXS          = "tree.i%d.child_idx"
TREE_NODE_PERCENT        = "tree.i%d.percent"
TREE_NODE_BASES          = "tree.i%d.bases"
TREE_INCREMENT_COMMENT   = "tree.i%d.comment"
TREE_INCREMENT_STARTED   = "tree.i%d.started"
TREE_INCREMENT_FINALIZED = "tree.i%d.finalized"
INCREMENT_BASE_FALLOFF_FACTOR = 0.1
INCREMENT_REBASE_THRESHOLD = 0.5

#======================================
# class: IncrementHandlerInterface
#======================================
class IncrementHandlerInterface:
	"""
	The user must implement this interface!
	"""
	def __init__(self,idx):
		self.idx = idx
		self.bases = []
		self.scan_bases = []
	def set_bases(self,bases):
		self.bases = bases
	def set_scan_bases(self,bases):
		self.scan_bases = bases
	"""
	The following methods must be implemented
	"""
	def remove_fs(self,fs_idx):
		pass
	def rebase_fs(self,bases):
		pass

#======================================
# class: IncrementTreeNode
#======================================
class IncrementTreeNode:
	"""
	Contains the info of one node of the increment tree
	"""
	def __init__(self,tree,idx,bases,scan_bases):
		self.tree = tree
		self.idx = idx
		self.bases = bases
		self.scan_bases = scan_bases
#======================================
# class: IncrementTree
#======================================
class IncrementTree:
	"""
	DB contains the following data:
	1. last_increment - the ID of the last unfinalized increment
	2. last_finalized_increment - the ID of the last finalized increment.
	   note that last_increment >= last_finalized_increment
	3. For increment x:
	   3.1 i_$x_bases - the list of the bases of this increment
	   3.2 i_$x_change - percent of change of this increment compared to the
	                    last base
	"""
	def __init__(self, db):
		self.db = db
		self.cur_increment = None
		# Create the root, dummy, increment
		if not self.db.has_key(TREE_NEXT_INCREMENT):
			self.db[TREE_NEXT_INCREMENT] = "0"
			self.db[TREE_FIRST_UNFINALIZED] = "0"
			self.__create_node(0,[])
			

	def start_increment(self,comment):
		"""
		Start a new increment.

		Returns a node representing the new increment.
		The increment has a list of bases and a list of scan bases.
		- The bases are those increments that this one is based upon,
		  including possibly the one that has just become a new bae.
		- The scan_bases are all the later increments that might contain
		  information relevant for this increment. These include:
		  1. All the bases of the node.
		  2. The last finalized increment, if it is not in the list of bases already
		  3. All the unfinalized increments after the last finalized one.
		"""
		if self.cur_increment != None:
			raise Exception("Must finalize an increment before starting a new one")

		#
		# Find the increments that the increment is based upon
		#
		l_bases = []
		scan_bases = []
		l_f_increment = int(self.db[TREE_FIRST_UNFINALIZED])-1
		if l_f_increment == -1:
			# No increment has been finalized yet, so nothing
			# to base on
			pass
		else:
			# Reuse the last finalized increment
			l_bases += self.__get_bases(l_f_increment)
			# Decide if the last finalized increment can become a base too
			l_min_percent = INCREMENT_BASE_FALLOFF_FACTOR ** (len(l_bases)+1)
			l_percent = self.__get_percent(l_f_increment)
			if l_percent >= l_min_percent:
				l_bases += [l_f_increment]
			else:
				# If the last finalized increment is not a base, it
				# still must be scanned
				scan_bases += [l_f_increment]

		#
		# Create the new increment node
		#
		self.cur_increment = int(self.db[TREE_NEXT_INCREMENT])
		self.db[TREE_NEXT_INCREMENT] = str(self.cur_increment+1)
		self.__create_node(self.cur_increment,l_bases)

		#
		# Find the increments that this increment can scan from
		#
		# scan_bases are all the unfinalized bases
		scan_bases += range(l_f_increment+1,self.cur_increment)

		self.db[TREE_INCREMENT_COMMENT%self.cur_increment] = comment
		return IncrementTreeNode(self,self.cur_increment,l_bases,scan_bases)
	def finalize_increment(self,percent_change,handler):
		"""
		Finalize the started increment.
		Commands the supplied handler object to perform database
		cleanup and/or to perform rebasing of increments.
		Returns whether finalize_increment needs to be performed again.
		"""
		assert self.cur_increment is not None
		#
		# The unfinalized increments are no longer necessary. Remove them
		#
		f_increment = int(self.db[TREE_FIRST_UNFINALIZED])
		for idx in range(f_increment,self.cur_increment):
			self.__remove_node(idx)
			handler.remove_increment(idx)
			
		self.db[TREE_FIRST_UNFINALIZED] = str(self.cur_increment+1)
		#
		# Decide if we want to rebase the increment.
		#
		self.__set_percent(self.cur_increment,percent_change)
		while True:
			bases = self.__get_bases(self.cur_increment)
			if len(bases) == 0:
				# Increment 0 is dummy, parent of everybody!
				assert percent_change == 1.0
				break
			min_percent = INCREMENT_BASE_FALLOFF_FACTOR ** (len(bases)-1)
			child_percent = self.__get_child_percent(bases[-1])
			#print "min_percent", min_percent, "child_percent", child_percent
			if child_percent >= INCREMENT_REBASE_THRESHOLD*min_percent:
				self.__rebase_node(self.cur_increment,bases[:-1])
				percent_change = handler.rebase_fs(bases[:-1])
				self.__set_percent(self.cur_increment,percent_change)

				#
				# Make sure all our assumptions on change percent hold
				#
				assert percent_change >= self.__get_percent(bases[-1])
			else:
				break
		#
		# Finish
		#
		self.cur_increment = None
		return None
	def is_increment_finalized(self,idx):
		assert(self.db.has_key(TREE_NODE_BASES%idx))
		f_increment = int(self.db[TREE_FIRST_UNFINALIZED])
		return idx < f_increment
	def get_comment(self,idx):
		return self.db[TREE_INCREMENT_COMMENT%idx]
	def info(self):
		#print self.db
		n_increment = int(self.db[TREE_NEXT_INCREMENT])
		for idx in range(0,n_increment):
			if not self.db.has_key(TREE_CHILD_IDXS%idx):
				continue
			prefix = " |"*self.__get_level(idx)
			print prefix+"-Increment", idx, "[", self.get_comment(idx), "] bases", self.__get_bases(idx), "change", self.__get_percent(idx)
	#-----------------------------------------------------------------
	# Utility functions for maintaining the tree structure as kept in DB
	#
	# Tree structure:
	# Each node keeps the following data:
	# TREE_NODE_BASES: list of its bases, including its immediate parent
	# TREE_CHILD_IDXS: list of its children. Must be consistent with TREE_NODE_BASES
	# TREE_NODE_PERCENT: percent of change of the current node from its base
	#-----------------------------------------------------------------
	def __create_node(self,idx,bases):
		self.db[TREE_CHILD_IDXS%idx] = ""
		self.db[TREE_NODE_BASES%idx] = " ".join(str(x) for x in bases)
		self.db[TREE_NODE_PERCENT%idx] = str(0.0)
		if len(bases)>0:
			self.__connect_child(bases[-1],idx)
	def __remove_node(self,idx):
		bases = self.__get_bases(idx)
		if len(bases)>0:
			self.__disconnect_child(bases[-1],idx)
		del self.db[TREE_NODE_BASES%idx]
		del self.db[TREE_CHILD_IDXS%idx]
		del self.db[TREE_NODE_PERCENT%idx]
	def __rebase_node(self,idx,bases):
		old_bases = self.__get_bases(idx)
		if len(old_bases)>0:
			self.__disconnect_child(old_bases[-1],idx)
		if len(bases)>0:
			self.__connect_child(bases[-1],idx)
		self.db[TREE_NODE_BASES%idx] = " ".join(str(x) for x in bases)
	#
	# Parent/Child relationship info
	#
	def __get_bases(self,idx):
		assert idx >= 0
		return [int(x) for x in self.db[TREE_NODE_BASES%idx].split()]
	def __get_level(self,idx):
		assert idx >= 0
		return len(self.__get_bases(idx))
	def __get_children(self,idx):
		assert idx >= 0
		return [int(x) for x in self.db[TREE_CHILD_IDXS%idx].split()]
	def __disconnect_child(self,parent_idx,child_idx):
		old_idxs = self.__get_children(parent_idx)
		idxs = [x for x in old_idxs if x!=child_idx]
		self.db[TREE_CHILD_IDXS%parent_idx] = " ".join(str(x) for x in idxs)
	def __connect_child(self,parent_idx,child_idx):
		idxs = self.__get_children(parent_idx) + [child_idx]
		self.db[TREE_CHILD_IDXS%parent_idx] = " ".join(str(x) for x in idxs)
	#
	# Percent handling
	#
	def __get_percent(self,idx):
		assert idx >= 0
		return float(self.db[TREE_NODE_PERCENT%idx])
	def __set_percent(self,idx,percent):
		assert idx >= 0
		assert percent >= 0.0 and percent <= 1.0
		self.db[TREE_NODE_PERCENT%idx] = str(percent)
	def __get_child_percent(self,idx):
		assert idx >= 0
		#print "Computing child percent for", idx,":"
		#print self.__get_children(idx)
		#print [self.__get_percent(child) for child in self.__get_children(idx)]
		return sum([self.__get_percent(child) for child in self.__get_children(idx)])
