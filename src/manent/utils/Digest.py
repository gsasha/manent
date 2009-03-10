#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import struct
import hashlib

#
# Hashing parameters
#
def dataDigest(data):
	h = hashlib.sha256(data)
	h.update(data)
	return h.digest()

class DataDigestAccumulator:
	def __init__(self):
		self.accum = hashlib.sha256()
	def update(self,data):
		self.accum.update(data)
	def digest(self):
		return self.accum.digest()

def dataDigestSize():
	return 32
	#return 16
