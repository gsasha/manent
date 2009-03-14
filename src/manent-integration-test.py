#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import filecmp
import logging
import os, os.path
import sys
import shutil
import stat
import subprocess
import tarfile
import tempfile

import manent.Config as Config

def cmpdirs(dir1, dir2):
  for name in os.listdir(dir1):
    if name == '.' or name == '..': continue
    file1 = os.path.join(dir1, name)
    file2 = os.path.join(dir2, name)
    if os.path.isfile(file1) and not filecmp.cmp(file1, file2):
      print "File %s differs in %s,%s" % (name, dir1, dir2)
      return False
    if os.path.isdir(file1):
      if not cmpdirs(file1, file2):
        return False
  return True

def set_writable(arg, dirname, names):
  # Set the files back to writable, otherwise, rmtree can't remove them on
  # native Windows.
  for name in names:
    os.chmod(os.path.join(dirname, name), stat.S_IWRITE)

tempdir = Config.paths.temp_area()
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
logging.info(" - Step 0 ---------------- Creating backup1")
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
logging.info(" - Step 1 ---------------- Testing pack 1")
tar = tarfile.open("testdata/pack1.tar")
tar.extractall(scratchdir)

backup1.scan(["comment=scan1"])
# TODO(gsasha): check that one container has been added
logging.info(" - Step 1 ---------------- Restoring from the backup and"
    "comparing the results")
backup1.restore(["storage=0",
  "increment=0",
  "target=%s" % restoredir.encode('utf8')])
assert(cmpdirs(scratchdir, restoredir))
for dir in [scratchdir, restoredir]:
  if os.name == 'nt':
    os.path.walk(dir, set_writable, None)
  shutil.rmtree(dir)
  os.mkdir(dir)
#
# Step2. Open second pack (which adds one image file), check that one container
# has been added.
#
logging.info("Testing second data pack")
tar = tarfile.open("testdata/pack2.tar")
tar.extractall(scratchdir)
backup1.scan(["comment=scan2"])
# TODO(gsasha): check that one more container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=1 target=%s" %
  restoredir.encode('utf8')).split())
assert(cmpdirs(scratchdir, restoredir))
for dir in [scratchdir, restoredir]:
  if os.name == 'nt':
    os.path.walk(dir, set_writable, None)
  shutil.rmtree(dir)
  os.mkdir(dir)

#
# Step3. Open third pack (which adds one large image file), check that
# one container has been added.
#
tar = tarfile.open("testdata/pack3.tar")
tar.extractall(scratchdir)
backup1.scan(["comment=scan3"])
# TODO(gsasha): check that one moore container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=2 target=%s" %
  restoredir.encode('utf8')).split())
assert(cmpdirs(scratchdir, restoredir))
for dir in [scratchdir, restoredir]:
  if os.name == 'nt':
    os.path.walk(dir, set_writable, None)
  shutil.rmtree(dir)
  os.mkdir(dir)

#
# Step3.5. Backup a lot of data to make sure there is something to piggyback.
#
for i in range(30):
  file = open(os.path.join(scratchdir, "file"+str(i)), "w")
  file.write(os.urandom(1024*1024))
  file.close()
backup1.scan(["comment=scan4"])
backup1.restore(("storage=0 increment=3 target=%s" %
  restoredir.encode('utf8')).split())
assert(cmpdirs(scratchdir, restoredir))
for dir in [scratchdir, restoredir]:
  if os.name == 'nt':
    os.path.walk(dir, set_writable, None)
  shutil.rmtree(dir)
  os.mkdir(dir)
backup1.close()

#
# Step4. Create another backup in same directory, and move it also through steps
# 1, 2, 3. Make sure that very small containers are created this time.
#
logging.info("Creating backup2")
label2 = "backup2"
backup2 = config.create_backup(label2)
config.save()
backup2.configure(
    ["add_storage", "type=directory", "path=%s" % storagedir, "encryption_key=kukuriku"])
backup2.configure(("set data_path=%s" % (scratchdir)).split())

backup2.scan(["comment=scan1"])
backup2.restore(("storage=0 increment=0 target=%s" %
  restoredir.encode('utf8')).split())
assert(cmpdirs(scratchdir, restoredir))
for dir in [scratchdir, restoredir]:
  if os.name == 'nt':
    os.path.walk(dir, set_writable, None)
  shutil.rmtree(dir)
  os.mkdir(dir)
backup2.close()
