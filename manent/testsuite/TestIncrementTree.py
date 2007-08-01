import unittest
import random
from cStringIO import StringIO

from manent.IncrementTree import *

class IncrementHandler(IncrementHandlerInterface):
	def __init__(self, expected_removed_increments,rebase_percents=[]):
		self.expected_removed_increments = expected_removed_increments
		self.rebase_percents = rebase_percents
		self.removed_increments = []
	def remove_increment(self,idx):
		self.removed_increments.append(idx)
	def rebase_fs(self,bases):
		percent = self.rebase_percents[len(bases)]
		if percent == None:
			raise Exception("Unexpected problem!")
		return percent

class TestIncrementTree(unittest.TestCase):
	def setUp(self):
		self.db = {}
	def test_starts_only(self):
		"""
		Test only starting increments, without finalizing them.
		"""
		itree = IncrementTree(self.db)
		#print
		#print "*** result before test_starts_only"
		#itree.info()
		node = itree.start_increment()
		self.assertEquals(node.idx,0)
		self.assertEquals(node.bases,[])
		self.assertEquals(node.scan_bases,[])
		# We can't start a new increment before finalizing the existing one.
		# If we want to do so, we must create a new itree, its state will be
		# created through the db!
		self.assertRaises(Exception,itree.start_increment,[])
		itree = IncrementTree(self.db)
		node = itree.start_increment()
		self.assertEquals(node.idx,1)
		self.assertEquals(node.bases,[])
		self.assertEquals(node.scan_bases,[0])
		# Do that again!
		itree = IncrementTree(self.db)
		node = itree.start_increment()
		self.assertEquals(node.idx,2)
		self.assertEquals(node.bases,[])
		self.assertEquals(node.scan_bases,[0,1])
		# Now finalize, and see that the previous increments get removed
		ih = IncrementHandler([0,1])
		itree.finalize_increment(1.0, ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		#print "*** result after test_starts_only:"
		#itree.info()
	def test_start_finalize(self):
		"""
		Test one start and finalize of an increment, to see that
		the next one is based on the first.
		"""
		itree = IncrementTree(self.db)
		node = itree.start_increment()
		self.assertEquals(node.idx,0)
		self.assertEquals(node.bases,[])
		self.assertEquals(node.scan_bases,[])
		ih = IncrementHandler([])
		itree.finalize_increment(1.0, ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		#
		# Start another increment
		#
		node = itree.start_increment()
		self.assertEquals(node.idx,1)
		self.assertEquals(node.bases,[0])
		self.assertEquals(node.scan_bases,[])
		itree.finalize_increment(0.3, ih)
		ih = IncrementHandler([])
		self.assertEquals(ih.removed_increments,ih.expected_removed_increments)
		#
		# Make an unfinalized increment with few changes - must be removed when
		# the next increment is finalized
		#
		node = itree.start_increment()
		self.assertEquals(node.idx,2)
		self.assertEquals(node.bases,[0,1])
		self.assertEquals(node.scan_bases,[])
		#
		# Now make a finalized increment with small changes
		#
		itree = IncrementTree(self.db)
		node = itree.start_increment()
		self.assertEquals(node.idx,3)
		self.assertEquals(node.bases,[0,1])
		self.assertEquals(node.scan_bases,[2])
		ih = IncrementHandler([2])
		itree.finalize_increment(0.01, ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		#
		# Now make a finalized increment with lots of changes.
		# See that it is rebased correctly
		#
		node = itree.start_increment()
		self.assertEquals(node.idx,4)
		self.assertEquals(node.bases,[0,1,3])
		self.assertEquals(node.scan_bases,[])
		ih = IncrementHandler([],[1.0,0.9,0.85])
		itree.finalize_increment(0.8, ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		#
		# Make a finalized increment with a very small change, and see that
		# it appears as scan base for the next increment, but not as a base
		#
		node = itree.start_increment()
		self.assertEquals(node.idx,5)
		self.assertEquals(node.bases,[4])
		self.assertEquals(node.scan_bases,[])
		ih = IncrementHandler([],[])
		itree.finalize_increment(0.001,ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		# the next increment should not be based on 5!
		node = itree.start_increment()
		self.assertEquals(node.idx,6)
		self.assertEquals(node.bases,[4])
		self.assertEquals(node.scan_bases,[5])
		ih = IncrementHandler([],[])
		itree.finalize_increment(0.002,ih)
		self.assertEquals(ih.removed_increments, ih.expected_removed_increments)
		
		#
		# TODO: what do we test by this?
		#
		change = 0.01
		for i in range(0,20):
			info = itree.start_increment()
			ih = IncrementHandler([],[1.0,0.0935+change])
			itree.finalize_increment(0.09+change,ih)
			change += 0.01
		#print "*** result after test_starts_finalize:"
		#itree.info()
