#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import re
import shutil
import StringIO
import subprocess
import sys
import tempfile
import traceback
import unittest

def system_checked(command):
  print "Executing", command
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
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

# TODO:
# 2. fix all the files in the testsuite to use the correct path calculationn
# 3. Add calls to integration test here.
# 4. Ask to write the release description blurb.
PREV_VERSION = read_version("src/manent/__init__.py")
if PREV_VERSION is None:
  print "Cannot read previous version"
  exit(1)
print "Current version %s" % PREV_VERSION

VERSION = sys.argv[1]

try:
  branch = [l for l in os.popen("git branch --no-color -v", "r")
      if l.startswith("* ")][0]
  branch = branch.split()[1]
except:
  print "You are not in a git repository"
  sys.exit(1)

diff = system_checked("git diff")
if diff != "":
  print "Git repository is not clean"
  sys.exit(1)

print "Running version tests. Don't forget to run the same tests on all the configurations"
ostream = StringIO.StringIO()
logging.basicConfig(stream=ostream)
import src.Unittest as Unittest
try:
  result = unittest.TextTestRunner(verbosity=1).run(Unittest.suite)
  if not result.wasSuccessful():
    print "Unit tests failed"
    print ostream.getvalue()
    sys.exit(1)
except:
  print "Problem running unit tests"
  traceback.print_exc()
  sys.exit(1)

changelog = system_checked(
    "git log --pretty=oneline V%s..HEAD" % PREV_VERSION)
print changelog

print "About to tag version %s on branch %s" % (VERSION, branch)

print "Type 'yes' if you are sure>",
line = sys.stdin.readline()
if line != 'yes\n':
  print "Ok, aborting"
  sys.exit(0)

SCRATCH = tempfile.mkdtemp()
PARAMS = {'VERSION': VERSION, 'SCRATCH': SCRATCH}

try:
  replace_version("src/manent/__init__.py", VERSION)
  system_checked("git tag V%(VERSION)s" % PARAMS)
  system_checked("git commit -a -m 'Tagged version %(PARAMS)s'" % PARAMS)
except:
  system_checked("git reset --hard HEAD")

system_checked(
    "git archive --format=tar --prefix=manent-%(VERSION)s/ "
    "V%(VERSION)s | tar xv -C %(SCRATCH)s" % PARAMS)

# Pack the commercial version
shutil.copyfile(
    "scripts/LICENSE-commercial.txt",
    "%(SCRATCH)s/manent-%(VERSION)s/LICENSE.txt" % PARAMS)
system_checked(
    "tar c -C %(SCRATCH)s manent-%(VERSION)s | bzip2 > "
    "releases/manent-commercial-%(VERSION)s.tar.bz2" % PARAMS)
system_checked(
    "cd %(SCRATCH)s; zip -q -r manent-commercial-%(VERSION)s.zip "
    "manent-%(VERSION)s" % PARAMS)
shutil.move(
    "%(SCRATCH)s/manent-commercial-%(VERSION)s.zip" % PARAMS,
    "releases")

# Pack the free version
shutil.copyfile(
    "scripts/LICENSE-gpl.txt",
    "%(SCRATCH)s/manent-%(VERSION)s/LICENSE.txt" % PARAMS)
system_checked(
    "tar c -C %(SCRATCH)s manent-%(VERSION)s | bzip2 > "
    "releases/manent-free-%(VERSION)s.tar.bz2" % PARAMS)
system_checked(
    "cd %(SCRATCH)s; zip -q -r manent-free-%(VERSION)s.zip "
    "manent-%(VERSION)s" % PARAMS)
shutil.move(
    "%(SCRATCH)s/manent-free-%(VERSION)s.zip" % PARAMS,
    "releases")

# Upload the version
system_checked(
    "scp -oPort=202 releases/manent-commercial-%(VERSION)s* "
    "releases/manent-free-%(VERSION)s* "
    "manent@manent-backup.com:/home/manent/public_html/downloads" % PARAMS)

