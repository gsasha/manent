import sys, os
import base64
from cStringIO import StringIO
import struct

from Block import Block

class BlockCache:
	"""
	Manage loading and unloading medias from containers
	"""
	def __init__(self,backup):
		self.backup = backup

		# These two databases are scratch-only, so they don't need to reliably survive
		# through program restarts
		self.requested_blocks = self.backup.global_config.get_database("manent-scratch."+self.backup.label, ".blocks", False)
		self.loaded_blocks = self.backup.global_config.get_database("manent-scratch."+self.backup.label, ".data", False)
		#
		# It is possible that the program was terminated before the scratch cache was
		# removed. In that case, it contains junk data
		#
		self.requested_blocks.truncate()
		self.loaded_blocks.truncate()
		
		self.containers = None

		self.loaded_size = 0
		self.max_loaded_size = 0
	def close(self):
		self.requested_blocks.close()
		self.requested_blocks.remove()
		self.loaded_blocks.close()
		self.loaded_blocks.remove()
	#
	# Methods for the user side of the cache
	#
	def request_block(self,digest):
		"""
		Used for preprocessing, to make all the future needed blocks known - this is to avoid
		reloading containers unnecessarily.
		"""
		if self.requested_blocks.has_key(digest):
			self.requested_blocks[digest] = str(int(self.requested_blocks[digest])+1)
		else:
			self.requested_blocks[digest] = "1"
	def analyze(self):
		"""
		Must be called after all the requests have been posted, to precompute
		the precedence order of the containers
		"""
		self.containers = {}
		for key,count in self.requested_blocks:
			block = Block(self.backup,key)
			for idx in block.containers:
				if self.containers.has_key(idx):
					self.containers[idx] = self.containers[idx]+int(count)
				else:
					self.containers[idx] = int(count)
	def load_block(self,digest,container_idx = None):
		"""
		Actually perform loading of the block. Assumes that the block
		was reported by request_block, and was loaded not more times than it was
		requested.
		"""
		block = Block(self.backup,digest)
		
		if not self.loaded_blocks.has_key(digest):
			if container_idx == None:
				# Load the block
				candidates = [(self.containers[x],x) for x in block.containers]
				candidates.sort()
				#print "Candidate containers", candidates
				container_idx = candidates[-1][1] # select the container with max refcount
				#print "loading block from container", container_idx
			container = self.backup.container_config.get_container(container_idx)
			self.backup.container_config.load_container_data(container_idx)
			report = container.read_blocks(self)

		data = self.loaded_blocks[digest]
		
		rcount = int(self.requested_blocks[digest])
		if rcount > 1:
			self.requested_blocks[digest] = str(rcount-1)
		else:
			self.loaded_size -= len(data)
			del self.requested_blocks[digest]
			del self.loaded_blocks[digest]

		for idx in block.containers:
			self.containers[idx] -= 1

		return data
	#
	# Methods for the supplier side of the cache
	#
	def block_needed(self,digest):
		if self.loaded_blocks.has_key(digest):
			return False
		return self.requested_blocks.has_key(digest)
	def block_loaded(self,digest,data):
		if self.loaded_blocks.has_key(digest):
			raise "Reloading block %s that is already in the cache" % base64.b64encode(digest)
		self.loaded_blocks[digest] = data
		self.loaded_size += len(data)
		if self.max_loaded_size < self.loaded_size:
			self.max_loaded_size = self.loaded_size
