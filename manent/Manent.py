#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

#
# TODO:
# - optimize the scanning: read each file information just once and do queries
#   on the results of stat rather than asking several questions on the same filename
# - remember to destroy the database environment if the program exits normally.
#

import logging
import os, os.path
import sys

import manent.Backup as Backup
import manent.Config as Config

config = Config.GlobalConfig()
config.load()

#
#  Print help message
#
if (len(sys.argv)==1) or (sys.argv[1]=="help"):
	print "Possible commands: create, configure, backup, info, restore"
	print "Available backups:"
	for label in config.list_backups():
		print "  ", label
	sys.exit(0)
#
#  Create a new backup set
#
elif sys.argv[1] == "create":
	label = sys.argv[2]

	if config.has_backup(label):
		print "Backup config", label, "already exists"
		sys.exit(0)
	backup = config.create_backup(label)

	config.save()
	config.close()
#
# Reconstruct the backup set from medias
#
elif sys.argv[1] == "configure":
	label = sys.argv[2]
	if not config.has_backup(label):
		print "Backup config", label, "does not exist"
		sys.exit(0)
	backup = config.load_backup(label)
	backup.configure(sys.argv[3:])

	config.save()
	config.close()
#
#  Do the backup
#
elif sys.argv[1] == "backup":
	label = sys.argv[2]

	backup = config.load_backup(label)
	backup.scan("stam")
	
	config.save()
	config.close()
#
# Do the restoration
#
elif sys.argv[1] == "restore":
	label = sys.argv[2]

	backup = config.load_backup(label)
	backup.restore(sys.argv[3:])
	config.close()

elif sys.argv[1] == "test":
	label = sys.argv[2]

	backup = config.load_backup(label)
	backup.test(sys.argv[3:])
	config.close()

elif sys.argv[1] == "remove":
	label = sys.argv[2]
	config.remove_backup(label)
	config.save()
	config.close()

elif sys.argv[1] == "info":
	label = sys.argv[2]
	backup = config.load_backup(label)
	backup.info(sys.argv[3:])
	#config.save()
	config.close()

else:
	print "Unknown command", sys.argv[1]

sys.exit(0)

