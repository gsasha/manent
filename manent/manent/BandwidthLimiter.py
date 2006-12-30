import time

class BandwidthLimiter:
	def __init__(self,speed_limit):
		self.packets = []
		self.size = 0
		self.speed_limit = speed_limit
		self.measured_speed = 0.0
	def packet(self,size):
		#print "packets",self.packets
		if len(self.packets) < 2:
			self.packets.append((time.time(),size))
			self.size = size
			return
		self.packets.append((time.time(),size))
		self.size += size
		time_range = self.packets[-1][0] - self.packets[0][0]
		while (time_range > 60.0) or self.size > 100*1024:
			self.size -= self.packets[0][1]
			self.packets = self.packets[1:]
			time_range = self.packets[-1][0] - self.packets[0][0]
		if self.size < 10*1024 and time_range < 1.0:
			#print "size too small"
			return
		#if time_range < 30.0 and self.size > 10*1024:
			#print "time too short"
			#return
		self.measured_speed = self.size / time_range
		if self.measured_speed > self.speed_limit:
			packet_delay = self.size/self.speed_limit - time_range
			#print "sleeping for",packet_delay,"seconds"
			time.sleep(packet_delay)
	def get_measured_speed(self):
		return self.measured_speed

#b = BandwidthLimiter(500000000000000.0)
#while(1):
	#b.packet(1024)
