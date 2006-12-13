import sys, os
import base64
from cStringIO import StringIO
import struct
import manent.utils.Format as Format

class Block:
	def __init__(self,backup,digest):
		self.backup = backup
		self.digest = digest

		if not self.backup.blocks_db.has_key(self.digest):
			self.containers = []
			return
		data = self.backup.blocks_db[self.digest]
		blockFile = StringIO(data)
		self.containers = Format.read_ints(blockFile)
	def add_container(self,container):
		self.containers.append(container)
	def save(self):
		result = StringIO()
		Format.write_ints(result,self.containers)
		self.backup.blocks_db[self.digest] = result.getvalue()
