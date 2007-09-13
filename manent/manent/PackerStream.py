from StreamAdapter import *
import manent.utils.Digest as Digest
import Container

class PackerOStream(OStreamAdapter):
	"""
	This ostream writes its data to a stream of containers
	"""
	def __init__(self,backup,code):
		OStreamAdapter.__init__(self, backup.blockSize())
		self.backup = backup
		self.code = code
		# The packer of a packer is a packer itself :)
		self.code_packer = Container.compute_packer_code(code)
		self.digests = []
		self.rec_ostream = None
	def write_block(self,data):
		digest = Digest.dataDigest(data)
		self.backup.add_block(digest,data,self.code)
		if self.rec_ostream is not None:
			self.rec_ostream.write(digest)
		else:
			self.digests.append(digest)
			if Digest.dataDigestSize()*len(self.digests) > self.backup.blockSize():
				self.rec_ostream = PackerOStream(self.backup,self.code_packer)
				for digest in self.digests:
					self.rec_ostream.write(digest)
				self.digests = None
	def get_digest(self):
		self.flush()
		if self.rec_ostream is not None:
			return self.rec_ostream.get_digest()
		
		digests_str = "".join(self.digests)
		digest = Digest.dataDigest(digests_str)
		self.backup.add_block(digest,digests_str,self.code_packer)
		return digest

class BlockReaderIStream(IStreamAdapter):
	"""Utility class for PackerIStream"""
	def __init__(self,backup,digest_stream):
		IStreamAdapter.__init__(self)
		self.backup = backup
		self.digest_stream = digest_stream
	def read_block(self):
		digest = self.digest_stream.read(Digest.dataDigestSize())
		if len(digest) == 0:
			return ""
		return self.backup.load_block(digest)

class PackerDigestLister:
	def __init__(self,backup,digest):
		self.backup = backup

		digest_stream = StringIO(digest)
		cur_reader = BlockReaderIStream(self.backup,digest_stream)
		while True:
			first_digest = cur_reader.read(Digest.dataDigestSize())
			if first_digest == '':
				break
			cur_reader.unread(first_digest)
			if Container.is_packer_code(self.backup.block_code(first_digest)):
				# Need one more level of reading
				cur_reader = BlockReaderIStream(self.backup,cur_reader)
			else:
				break
		self.block_istream = cur_reader
	def __iter__(self):
		return self
	def next(self):
		digest = self.block_istream.read(Digest.dataDigestSize())
		if len(digest) == 0:
			raise StopIteration
		return digest

class PackerIStream(IStreamAdapter):
	"""
	This istream reads its data from a stream of containers
	"""
	def __init__(self,backup,digest):
		IStreamAdapter.__init__(self)
		
		self.backup = backup
		self.digest_lister = PackerDigestLister(self.backup,digest)
	def read_block(self):
		try:
			digest = self.digest_lister.next()
			return self.backup.load_block(digest)
		except StopIteration:
			return ""
