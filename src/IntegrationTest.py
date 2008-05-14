#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os, os.path
import sys
import shutil
import subprocess
import tempfile

tempdir = tempfile.mkdtemp(prefix="/tmp/")
homedir = os.path.join(tempdir, "home")
storagedir = os.path.join(tempdir, "storage")
scratchdir = os.path.join(tempdir, "scratch")
restoredir = os.path.join(tempdir, "restore")

for dir in homedir, storagedir, scratchdir, restoredir:
  os.mkdir(dir)

os.environ["MANENT_HOME_DIR"] = homedir
import manent.Backup as Backup
import manent.Config as Config
config = Config.GlobalConfig()
config.load()

logging.info("Running integration testing in " + tempdir)
logging.info("homedir=%s, storagedir=%s, scratchdir=%s" %
    (homedir, storagedir, scratchdir))

datadir = "testdata"
#
# Step 0. Create backup 1, configure it and run one backup iteration. Check that
# one container has been added.
#
logging.info("Creating backup1")
label1 = "backup1"
backup1 = config.create_backup(label1)
config.save()
backup1.configure(
    ("add_storage type=directory path=%s encryption_key=kukuriku" %
      (storagedir)).split())
backup1.configure(("set data_path=%s" % (scratchdir)).split())
#
# Step 1. Test backup&restore of the first data pack.
#
retcode = subprocess.call("tar xvf testdata/pack1.tar -C %s" % scratchdir,
    shell=True)
assert retcode == 0
backup1.scan("scan1")
# TODO(gsasha): check that one container has been added
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=0 target=%s" % restoredir).split())
retcode = subprocess.call("diff -r %s %s" % (scratchdir, restoredir),
    shell=True)
assert retcode == 0
for dir in [scratchdir, restoredir]:
  shutil.rmtree(dir)
  os.mkdir(dir)
#
# Step2. Open second pack (which adds one image file), check that one container
# has been added.
#
logging.info("Testing second data pack")
os.system("tar xvf testdata/pack2.tar -C %s" % scratchdir)
backup1.scan("scan2")
# TODO(gsasha): check that one more container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=1 target=%s" % restoredir).split())
retcode = subprocess.call("diff -r %s %s" % (scratchdir, restoredir),
    shell=True)
assert retcode == 0
for dir in [scratchdir, restoredir]:
  shutil.rmtree(dir)
  os.mkdir(dir)

#
# Step3. Open third pack (which adds one large image file), check that
# one container has been added.
#
os.system("tar xvf testdata/pack3.tar -C %s" % scratchdir)
backup1.scan("scan3")
# TODO(gsasha): check that one moore container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=2 target=%s" % restoredir).split())
retcode = subprocess.call("diff -r %s %s" % (scratchdir, restoredir),
    shell=True)
assert retcode == 0
for dir in [scratchdir, restoredir]:
  shutil.rmtree(dir)
  os.mkdir(dir)

#
# Step3.5. Backup a lot of data to make sure there is something to piggyback.
#
for i in range(30):
  file = open(os.path.join(scratchdir, "file"+str(i)), "w")
  file.write(os.urandom(1024*1024))
  file.close()
backup1.scan("scan4")
backup1.restore(("storage=0 increment=3 target=%s" % restoredir).split())
retcode = subprocess.call("diff -r %s %s" % (scratchdir, restoredir),
    shell=True)
assert retcode == 0
for dir in [scratchdir, restoredir]:
  shutil.rmtree(dir)
  os.mkdir(dir)


#
# Step4. Create another backup in same directory, and move it also through steps
# 1, 2, 3. Make sure that very small containers are created this time.
#
logging.info("Creating backup2")
label2 = "backup2"
backup2 = config.create_backup(label2)
config.save()
backup2.configure(
    ("add_storage type=directory path=%s encryption_key=kukuriku" %
      (storagedir)).split())
backup2.configure(("set data_path=%s" % (scratchdir)).split())

backup2.scan("scan1")
backup2.restore(("storage=0 increment=0 target=%s" % restoredir).split())
retcode = subprocess.call("diff -r %s %s" % (scratchdir, restoredir),
    shell=True)
assert retcode == 0
for dir in [scratchdir, restoredir]:
  shutil.rmtree(dir)
  os.mkdir(dir)

