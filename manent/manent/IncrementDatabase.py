from Increment import *

class IncrementDatabase:
	def __init__(self,repository,db):
		self.repository = repository
		self.db = db

		self.last_completed_increment = None
		if self.db.has_key("IncrementDB.last_completed"):
			self.last_completed_increment = int(self.db["IncrementDB.last_completed"])
		
		self.last_intermediate_increment = None
		if self.db.has_key("IncrementDB.last_intermediate"):
			self.last_intermediate_increment = int(self.db["IncrementDB.last_intermediate"])
			
		self.active_increment = None

	def start_increment(self, comment):
		assert self.active_increment is None

		bases = []
		if self.last_intermediate_increment is None:
			if self.last_completed_increment is not None:
				bases = [self.last_completed_increment]
		elif self.last_completed_increment is None:
			bases = range(0,self.last_intermediate_increment+1)
		else:
			if self.last_intermediate_increment < self.last_completed_increment:
				bases = [self.last_completed_increment]
			else:
				bases = range(self.last_completed_increment,self.last_intermediate_increment+1)
		
		if len(bases) == 0:
			new_index = 0
		else:
			new_index = bases[-1]+1

		self.active_increment = Increment(self,self.repository,self.db)
		self.active_increment.start(new_index,comment)

	def finalize_increment(self,digest):
		assert self.active_increment is not None
		self.active_increment.finalize(digest)
		self.db["IncrementDB.last_completed"] = str(self.active_increment.get_index())
		self.active_increment = None

	def dump_intermediate(self,digest):
		assert self.active_increment is not None
		self.active_increment.dump_intermediate(digest)
		
		index = self.active_increment.get_index()
		comment = self.active_increment.get_comment()
		
		self.db["IncrementDB.last_intermediate"] = str(index)
		self.active_increment = Increment(self,self.repository,self.db)
		self.active_increment.start(index+1,comment)
