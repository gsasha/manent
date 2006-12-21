import md5
import struct
import hashlib

#
# Hashing parameters
#
def dataDigest(data):
	h = hashlib.sha256(data)
	#h = md5.new()
	h.update(struct.pack("B",len(data)%256))
	h.update(data)
	return h.digest()

def dataDigestSize():
	return 32
	#return 16

def headerDigest(data):
	return hashlib.md5(data).digest()
	#return md5.md5(data).digest()

def headerDigestSize():
	return 16
