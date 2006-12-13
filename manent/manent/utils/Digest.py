import md5
import struct

#
# Hashing parameters
#
def dataDigest(data):
	#return hashlib.sha256(data).digest()
	h = md5.new(struct.pack("B",len(data)%256))
	h.update(data)
	return h.digest()

def dataDigestSize():
	#return 32
	return 16

def headerDigest(data):
	#return hashlib.md5(data).digest()
	return md5.md5(data).digest()

def headerDigestSize():
	return 16
