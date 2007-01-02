import time

class BandwidthLimiter:
	def __init__(self,speed_limit):
		self.packets = []
		self.delays = []
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
		else:
			packet_delay = 0
		return

		self.delays.append(time.time(), packet_delay)
		# Count data transferred during last second
		last_time = self.packets[-1][0]
		second_size = 0
		for (t,s) in reversed(self.packets):
			if last_time - t > 1.0:
				break
			second_size += s
		while len(self.delays) > 0 and self.delays[-1][0]-self.delays[0][0] > 10.0:
			self.delays = self.delays[1:]
		total_delay = 0.0
		for t,d in self.delays:
			total_delay = total_delay*0.8+d*0.2
		#total_delay = sum([x[1] for x in self.delays])
		# Decide if we want to decrease or increase the limit
		if total_delay < 0.05:
			self.speed_limit = self.speed_limit / 1.03
			#print "decreasing speed limit to", self.speed_limit
		elif total_delay < 0.1:
			self.speed_limit = self.speed_limit / 1.015
			#print "decreasing speed limit to", self.speed_limit
		elif total_delay < 0.15:
			self.speed_limit = self.speed_limit / 1.007
			#print "decreasing speed limit to", self.speed_limit
		elif total_delay > 2.0:
			self.speed_limit = self.speed_limit * 1.05
		elif total_delay > 1.0:
			self.speed_limit = self.speed_limit * 1.015
		elif total_delay > 0.5:
			self.speed_limit = self.speed_limit * 1.007
		elif total_delay > 0.3:
			self.speed_limit = self.speed_limit * 1.003
		#elif self.measured_speed*1.2 > self.speed_limit:
			#self.speed_limit = self.speed_limit * 1.01
			##print "increasing speed limit to", self.speed_limit
		#print "total_delay",total_delay,"limit",self.speed_limit, "measured",self.measured_speed
		#print "delays", self.delays
	def get_measured_speed(self):
		return self.measured_speed

#b = BandwidthLimiter(500000000000000.0)
#while(1):
	#b.packet(1024)
