#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import re
import os
import shutil
import subprocess
import sys
import tempfile

def system_checked(command, cwd=None):
  print "Executing", command
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, cwd=cwd)
  sts = os.waitpid(p.pid, 0)
  if sts[1] != 0:
    print "Failed to execute", command, "status =", sts
    sys.exit(1)
  return p.stdout.read()

def read_version(file):
  for line in open(file):
    m = re.match("__version__ = \"(.*)\"", line)
    if m:
      return m.groups()[0]
  return None

def replace_version(file, version):
  lines = []
  for line in open(file, "r"):
    if line.startswith("__version__"):
      lines.append("__version__ = \"%s\"\n" % version)
    else:
      lines.append(line)
  fs = open(file, "w")
  for line in lines:
    fs.write(line)
  fs.close()

SCRATCH = tempfile.mkdtemp()
VERSION = sys.argv[1]
PARAMS = {'VERSION': VERSION, 'SCRATCH': SCRATCH}

build_path = tempfile.mkstemp()
system_checked(
    "git archive --format=tar --prefix=manent-%(VERSION)s/ "
    "V%(VERSION)s | tar xv -C %(SCRATCH)s" % PARAMS)

PARAMS["BZIP_OUT"] = os.path.abspath(
    "releases/manent-%(VERSION)s.tar.bz2" % PARAMS)
PARAMS["ZIP_OUT"] = os.path.abspath(
    "releases/manent-%(VERSION)s.zip" % PARAMS)

replace_version("%(SCRATCH)s/manent-%(VERSION)s/src/manent/__init__.py" %
    PARAMS, VERSION)
system_checked(
    "tar c -C %(SCRATCH)s manent-%(VERSION)s | bzip2 > %(BZIP_OUT)s" % PARAMS)
system_checked(
    "zip -r %(ZIP_OUT)s manent-%(VERSION)s" % PARAMS,
    cwd = SCRATCH)
