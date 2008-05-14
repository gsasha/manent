import base64
import logging
import os, os.path
import sys

import manent.Container as Container
import manent.Database as Database
import manent.Storage as Storage
import manent.utils.IntegerEncodings as IE

args = sys.argv[1:]
params = {}
while (len(args[0].split("=")) == 2):
	k, v = args[0].split("=")
	params[k] = v
	args = args[1:]

sequence_id = base64.urlsafe_b64decode(args[0])
idx = IE.ascii_decode_int_varlen(args[1])
print "Loading container", base64.urlsafe_b64encode(sequence_id), idx
env = Database.PrivateDatabaseManager()
config_db = env.get_database_btree("", None, None)

class Handler:
	def __init__(self):
		pass
	def report_new_container(self, container):
		pass

storage = Storage.DirectoryStorage(0, config_db)
storage.configure(params, Handler())

container = Container.Container(storage)
container.start_load(sequence_id, idx)
container.load_header()
container.load_body()
container.print_blocks()
