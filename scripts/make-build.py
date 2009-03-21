#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

# This script is intended to be executed from both Linux and Windows.
# From Linux, it will generate the release (based on setup.py).
# From Windows, it will generate the installer.
# On Windows, it will work only from within cygwin.
# The following commands need to be available from cygwin:
# tar, zip, git

import re
import os
import shutil
import subprocess
import sys
import tempfile

def system_checked(command, cwd=None):
  print "Executing", command
  p = subprocess.Popen(command, shell=True,
      stdout=subprocess.PIPE, cwd=cwd)
  sts = os.waitpid(p.pid, 0)
  if sts[1] != 0:
    print "Failed to execute", command, "status =", sts
    sys.exit(1)
  return p.stdout.read()

def replace_version(file, version):
  lines = []
  for line in open(file, "r"):
    line = line.replace("DEVELOPMENT", version)
    lines.append(line)
  fs = open(file, "w")
  for line in lines:
    fs.write(line)
  fs.close()

SCRATCH = tempfile.mkdtemp()
print 'Using scratch dir', SCRATCH
VERSION = sys.argv[1]
PARAMS = {'VERSION': VERSION, 'SCRATCH': SCRATCH}

build_path = tempfile.mkstemp()
system_checked(
    "git archive --format=tar --prefix=manent-%(VERSION)s/ "
    "V%(VERSION)s | tar xv -C %(SCRATCH)s" % PARAMS)

# Prepare the distribution
replace_version("%(SCRATCH)s/manent-%(VERSION)s/src/manent/__init__.py" %
    PARAMS, VERSION)

# Create the distribution files
PARAMS["RELEASE_DIR"] = os.path.abspath("releases")
PARAMS["BZIP_OUT"] = os.path.join(
    PARAMS["RELEASE_DIR"],
    "manent-%(VERSION)s.tar.bz2" % PARAMS)
PARAMS["ZIP_OUT"] = os.path.join(
    PARAMS["RELEASE_DIR"],
    "manent-%(VERSION)s.zip" % PARAMS)

system_checked(
    "tar c -C %(SCRATCH)s manent-%(VERSION)s | bzip2 > '%(BZIP_OUT)s'" % PARAMS)
system_checked(
    "zip -r '%(ZIP_OUT)s' manent-%(VERSION)s" % PARAMS,
    cwd=SCRATCH)

# Create the windows installer
INSTALLER_BUILD_DIR = os.path.join(SCRATCH,
    "manent-"+VERSION, "src")
system_checked(
    "c:/inst/python26/python.exe setup.py py2exe",
    cwd=INSTALLER_BUILD_DIR)
system_checked(
    '"c:/Program Files/Inno Setup 5/ISCC.exe" manent.iss '
    '/O. /Fmanent-setup-%(VERSION)s' % PARAMS,
    cwd=INSTALLER_BUILD_DIR)
shutil.copy(os.path.join(INSTALLER_BUILD_DIR,
  "manent-setup-%(VERSION)s.exe" % PARAMS),
  PARAMS["RELEASE_DIR"])

# Upload the distribution files
# Doesn't work under Windows, I'll have to debug it.
# system_checked(
#    "python scripts/googlecode_upload.py --project=manent "
#    "--user=gsasha@gmail.com --labels=Featured "
#    "'%(BZIP_OUT)s' --summary='Release %(VERSION)s'"
#    % PARAMS)
# system_checked(
#    "python scripts/googlecode_upload.py --project=manent "
#    "--user=gsasha@gmail.com --labels=Featured "
#   "'%(ZIP_OUT)s' --summary='Release %(VERSION)s'"
#    % PARAMS)

NOTIFICATION_ADDRESSES = [
    "update-windows@softpedia.com",
    "update-mac@softpedia.com",
    "update-linux@softpedia.com",
    "manent@googlegroups.com"]

print "Send notification mail to", ",".join(NOTIFICATION_ADDRESSES)
