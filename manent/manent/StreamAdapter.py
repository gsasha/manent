from cStringIO import StringIO

class IStreamAdapter:
	def __init__(self):
		#
		# The IStreamAdapter consists of two parts: buffer, which keeps a block
		# and stream
		#
		self.buf = None
		self.buf_size = 0
		self.stream = None
		self.stream_size = 0

	#
	# Functions for the user
	#
	def read(self,size=None):
		resultS = StringIO()
		result_size = 0

		while size is None or result_size < size:
			if self.buf == None:
				block = self.read_block()
				if len(block) == 0:
					break
				if len(block) <= (size-result_size):
					# Optimization: don't copy the data over to the stream
					# if all of it is going to be used immediately
					resultS.write(block)
					result_size += len(block)
					continue
				self.buf = StringIO(block)
			data = self.buf.read(size-result_size)
			if len(data) == 0:
				self.buf = None
				continue
			resultS.write(data)
			result_size += len(data)
		return resultS.getvalue()
			
	def unread(self,data):
		"""Return data to the stream, so it will be available again
		on next read"""
		if self.buf == None:
			self.buf = StringIO(data)
			return
		next_data = self.buf.read()
		self.buf = StringIO(data+next_data)

	def seek(self, offset, whence=1):
		if whence != 1:
			raise "Only whence=1 is supported"
		if offset < 0:
			raise "Can seek only forward"
		block = 1024*4
		while offset > block:
			data = self.read(block)
			if len(data) < block:
				raise "Not enough available data to seek"
			offset -= block
		if offset > 0:
			data = self.read(offset)
			if len(data) < offset:
				raise "Not enough data available to seek"
		#
		# we have read forward as much as was required.
		# so now, the stream is positioned correctly
		#
		return
	def readline(self):
		result = StringIO()
		while True:
			ch = self.read(1)
			if len(ch) == 0:
				break
			result.write(ch)
			if ch == "\n":
				break
		return result.getvalue()
	# Support for iterating over lines of a file
	def __iter__(self):
		return self
	def next(self):
		line = self.readline()
		if len(line)==0:
			raise StopIteration
		return line
	
	def read_block(self):
		raise Exception("read_block must be overridden by the inheriting class")

class OStreamAdapter:
	def __init__(self,max_block_size):
		self.max_block_size = max_block_size
		self.buf = StringIO()
		self.buflen = 0
		self.total = 0
	#
	# Interface for the user
	#
	def write(self,data):
		self.buf.write(data)
		self.buflen += len(data)
		self.total += len(data)
		while self.buflen > self.max_block_size:
			self.__write_chunk()
	def flush(self):
		while self.buflen > 0:
			self.__write_chunk()
	#
	# Interface for the inheriting class
	#
	def write_block(self,data):
		"""
		Inheriting class must override this
		"""
		raise Exception("write_block must be implemented!")
		
	#
	# Implementation
	#
	def __write_chunk(self):
		chunk = self.buflen
		if chunk > self.max_block_size:
			chunk = self.max_block_size
		buf = self.buf.getvalue()
		written = buf[0:chunk]
		self.buflen -= chunk
		if self.buflen > 0:
			self.buf = StringIO()
			self.buf.write(buf[chunk:])
		self.write_block(written)
