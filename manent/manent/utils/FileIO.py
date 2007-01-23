import sys

#---------------------------------
# Utility method that reads a file as a sequence
# of blocks of given size
#---------------------------------
def read_blocks(file,blocksize):
	while True:
		block = file.read(blocksize)
		if len(block) == 0:
			break
		yield block
	raise StopIteration

#--------------------------------------------------------------------
# Support for file reading and writing with reporting and specified
# speed
#--------------------------------------------------------------------
class FileWriter:
	def __init__(self,filename,bandwidth_limiter):
		self.file = open(filename,"wb")
		self.bw_limiter = bandwidth_limiter
		self.total = 0
	def write(self,data):
		self.total += len(data)
		self.bw_limiter.packet(len(data))
		sys.stdout.write("%d \r"%self.total)
		sys.stdout.flush()
		return self.file.write(data)

class FileReader:
	def __init__(self,filename,bandwidth_limiter):
		self.bw_limiter = bandwidth_limiter
		#BandwidthLimiter.__init__(self,10000.0)
		self.file = open(filename,"rb")
		self.total = 0
	def read(self,size):
		self.total += size
		self.bw_limiter.packet(size)
		sys.stdout.write("%d speed:%3.0f limit:%3.0f\r" % (self.total,self.bw_limiter.get_measured_speed(),self.bw_limiter.speed_limit))
		sys.stdout.flush()
		return self.file.read(size)

