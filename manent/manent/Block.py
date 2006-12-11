import sys, os
import base64
from cStringIO import StringIO
import struct

class Block:
	def __init__(self,backup,digest):
		self.backup = backup
		self.digest = digest

		self.containers = []
		if not self.backup.blocks_db.has_key(self.digest):
			return
		data = self.backup.blocks_db[self.digest]
		blockFile = StringIO(data)
		while True:
			containerNum = self.backup.config.read_int(blockFile)
			if containerNum == None:
				break
			self.containers.append(containerNum)
	def add_container(self,container):
		self.containers.append(container)
	def save(self):
		result = StringIO()
		for container in self.containers:
			self.backup.config.write_int(result,container)
		self.backup.blocks_db[self.digest] = result.getvalue()
