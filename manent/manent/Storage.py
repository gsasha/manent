import threading

STATUS_QUEUED = "q"
STATUS_SAVING = "s"
STATUS_WAITING = "w"
STATUS_FAILED = "f"

class Storage:
	"""
	The base class for all storage locations.
	Provides the following services:

	1. Queueing of the requests to an outbox.
	2. Managing the thread that saves the container to the outgoing medium
	3. Writing back the result, updating the status of containers
	4. Managing the transfer of containers through alternative medium
	"""
	def __init__(self,backup):
		self.backup = backup
		self.containers = []
		self.requests_q = self.backup.db_config.get_database_queue("manent.queueTODO", "outbox.queue")
		self.response_q = self.backup.db_config.get_database_queue("manent.queueTODO", "response.queue")
		self.container_db = self.backup.db_config.get_database("manent.queueTODO", "storage")
		self.queue_size = 0 # TODO

		self.saver = SaverThread(self)
		class SaverThread(threading.Thread):
			def __init__(storage):
				threading.Thread.__init___()
				self.storage = Storage
				self.txn = self.backup.db_config.start_txn()
			def run(self):
				while True:
					container_file = self.storage.requests_q.get_wait()
					self.storage.upload_container(container_file)
					self.storage.response_q.append(container_file)
					self.txn.commit()
					self.txn = self.backup.db_config.start_txn()
	def container_status(self,container_idx):
		return "q"
	def add_container(self,container):
		"""
		Add container to the queue, from where the upload thread will take it.
		If the queue is too large, wait for it to get a bit smaller, as they get uploaded
		by the thread
		"""
		self.requests_q.append(container.idx)
		while self.queue_size > self.backup.global_config.max_outbox_size():
			(response_id,response) = self.response_q.get_wait()
			container_size = self.backup.container_config.get_container(int(response))
			# TODO: remove the container
			self.queue_size -= container_size
		pass
	def upload_container(self,container_idx):
		"""
		This is pure virtual method, to be implemented by derived classes
		"""
		raise "not implemented"
	def set_container_status(self,container_idx,status):
		self.container_db["container%d" % container_idx] = status
	def get_container_status(self,container_idx):
		return self.container_db["container%d" % container_idx]
