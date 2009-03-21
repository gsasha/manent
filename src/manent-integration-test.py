#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import errno
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


def reset_dir(dir):
  def set_writable(arg, dirname, names):
    # Set the files back to writable, otherwise, rmtree can't remove them on
    # native Windows.
    for name in names:
      os.chmod(os.path.join(dirname, name), stat.S_IWRITE)
  for dir in [scratchdir, restoredir]:
    if os.name == 'nt':
      os.path.walk(dir, set_writable, None)
    shutil.rmtree(dir)
    os.mkdir(dir)
def reset_dirs(dirs):
  for dir in dirs:
    reset_dir(dir)

def extract_tarfile_with_utf8(tf, target):
  """Extract the given tarfile object to the given directory, assuming that
  the file names are encoded as UTF8. At least in Python 2.5, there is no
  option of supplying the encoding"""
  for member in tf.getmembers():
    if not member.isfile():
      continue
    name = unicode(member.name, "utf8")
    path = os.path.join(target, name)
    try:
      os.makedirs(os.path.dirname(path))
    except OSError, (no, strerror):
      # Directory already exists. Oh well.
      if no == errno.EEXIST:
        pass
      else:
        raise
    of = open(path, "w")
    of.write(tf.extractfile(member).read())
    of.close()

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
print "Step 0"
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
print "Step 1"
logging.info(" - Step 1 ---------------- Testing pack 1")
tar = tarfile.open("testdata/pack1.tar")
extract_tarfile_with_utf8(tar, scratchdir)

backup1.scan(["comment=scan1"])
# TODO(gsasha): check that one container has been added
logging.info(" - Step 1 ---------------- Restoring from the backup and"
    "comparing the results")
backup1.restore(["storage=0",
  "increment=0",
  "target=%s" % restoredir.encode('utf8')])
assert(cmpdirs(scratchdir, restoredir))
reset_dirs([scratchdir, restoredir])

#
# Step2. Open second pack (which adds one image file), check that one container
# has been added.
#
print "Step 2"
logging.info("Testing second data pack")
tar = tarfile.open("testdata/pack2.tar")
extract_tarfile_with_utf8(tar, scratchdir)
print "--------------- 2.1"
backup1.scan(["comment=scan2"])
print "--------------- 2.2"
# TODO(gsasha): check that one more container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=1 target=%s" %
  restoredir.encode('utf8')).split())
print "--------------- 2.3"
assert(cmpdirs(scratchdir, restoredir))
reset_dirs([scratchdir, restoredir])
print "--------------- 2.4"

#
# Step3. Open third pack (which adds one large image file), check that
# one container has been added.
#
print "Step 3"
tar = tarfile.open("testdata/pack3.tar")
extract_tarfile_with_utf8(tar, scratchdir)
print "------------------- 1"
backup1.scan(["comment=scan3"])
print "------------------- 2"
# TODO(gsasha): check that one moore container has been added.
logging.info("Restoring from the backup and comparing the results")
backup1.restore(("storage=0 increment=2 target=%s" %
  restoredir.encode('utf8')).split())
print "------------------- 3"
assert(cmpdirs(scratchdir, restoredir))
reset_dirs([scratchdir, restoredir])

#
# Step3.5. Backup a lot of data to make sure there is something to piggyback.
#
print "Step 3.5"
for i in range(30):
  file = open(os.path.join(scratchdir, "file"+str(i)), "w")
  file.write(os.urandom(1024*1024))
  file.close()
backup1.scan(["comment=scan4"])
backup1.restore(("storage=0 increment=3 target=%s" %
  restoredir.encode('utf8')).split())
assert(cmpdirs(scratchdir, restoredir))
reset_dirs([scratchdir, restoredir])

backup1.close()

#
# Step4. Create another backup in same directory, and move it also through steps
# 1, 2, 3. Make sure that very small containers are created this time.
#
print "Step 4"
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
reset_dirs([scratchdir, restoredir])

backup2.close()
