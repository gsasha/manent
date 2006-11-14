from cStringIO import StringIO
import time

# --------------------------------------------------------------------
# CLASS: Increment
# --------------------------------------------------------------------
class Increment:
	def __init__(self,container_config,index):
		self.container_config = container_config
		self.index = index
		self.db = self.container_config.containers_db

		self.readonly = None
		self.finalized = False
		self.base_index = None
		self.base_diff = None
	def message(self):
		# TODO: Make the message include other data, comment etc.
		m = StringIO()
		m.write("Increment %d of backup %s\n" % (self.index, self.container_config.backup.label))
		m.write("version=%s\n" % self.container_config.backup.global_config.version())
		m.write("index=%d\n" % self.index)
		m.write("backup=%s\n" % self.container_config.backup.label)
		m.write("time=%s\n" % self.ctime)
		if self.base_index != None:
			m.write("base=%d\n" % self.base_index)
		m.write("\n")
		return m.getvalue()

	#
	# Methods for manipulating a newly created increment
	#
	def start(self,base_index):
		if self.readonly != None:
			raise "Attempting to edit an existing increment"
		self.readonly = False

		self.ctime = time.ctime()
		self.containers = []
		self.db["I%d.containers"%(self.index)] = str(len(self.containers))
		if base_index != None:
			self.base_index = base_index
			self.db["I%d.base_index"%(self.index)] = str(self.base_index)
	def add_container(self,index):
		if self.readonly != False:
			raise "Attempting to add container to a readonly increment"
		
		self.db["I%d.%d"%(self.index,len(self.containers))] = str(index)
		self.containers.append(index)
		self.db["I%d.containers"%(self.index)] = str(len(self.containers))
	def finalize(self,base_diff):
		if self.readonly != False:
			raise "Increment already finalized"

		if base_diff != None:
			if self.base_index == None:
				raise "setting base diff without index"
			self.base_diff = base_diff
			self.db["I%d.base_diff"%(self.index)] = str(self.base_diff)
		elif self.base_index != None:
			raise "base index set, but no base diff!"
		
		print "Finalizing increment",self.index, self.containers
		self.db["I%d.finalized"%(self.index)] = "1"
		self.finalized = True
		self.readonly = True
	#
	# Loading an existing increment from db
	#
	def load(self):
		if self.readonly != None:
			raise "Attempt to load an existing increment"
		
		num_containers = int(self.db["I%d.containers"%(self.index)])
		self.containers = []
		for i in range(0,num_containers):
			self.containers.append(int(self.db["I%d.%d"%(self.index,i)]))
		if self.db.has_key("I%d.finalized"%(self.index)):
			self.finalized = True
		if self.db.has_key("I%d.base_index"%(self.index)):
			self.base_index = int(self.db["I%d.base_index"%(self.index)])
			self.base_diff = float(self.db["I%d.base_diff"%(self.index)])

		self.readonly = True
	#
	# Restoring an increment from backup to db
	#
	def restore(self,message,start_container,end_container,is_finalized):
		if self.readonly != None:
			raise "Attempt to restore an existing increment"

		self.db["I%d.containers"%self.index] = str(end_container-start_container+1)
		num_containers = end_container-start_container+1
		self.containers = []
		for i in range(0,num_containers):
			self.db["I%d.%d"%(self.index,i)] = str(start_container+i)
			self.containers.append(i+start_container)

		if is_finalized:
			self.db["I%d.finalized"%self.index] = "1"
			self.finalized = True

		self.readonly = True
	def list_specials(self,code):
		result = []
		for idx in self.containers:
			container = self.container_config.get_container(idx)
			for (blockDigest,blockSize,blockCode) in container.blocks:
				if blockCode==code:
					result.append((idx,blockDigest))
		return result