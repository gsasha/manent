#!/usr/bin/env python

#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging
import os
import StringIO
import sys
import traceback
import unittest

# TODO:
# 1. make everybody read the version from the manent module
# 2. fix all the files in the testsuite to use the correct path calculationn
# 3. Add calls to integration test here.

VERSION = sys.argv[1]
PREV_VERSION = sys.argv[2]

def system_checked(command):
  retval = os.system(command)
  if retval != 0:
    print "Failed to execute", command
    sys.exit(1)

try:
  branch = [l for l in os.popen("git branch --no-color -v", "r")
      if l.startswith("* ")][0]
except:
  print "You are not in a git repository"
  sys.exit(1)
print "You are releasing version %s at %s" % (VERSION, branch),

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

changelog = os.popen("git log --pretty=oneline %s..%s" %
                     (PREV_VERSION, VERSION), "r").read()
print changelog,

sys.exit(0)

system_checked("git tag V%s" % VERSION,
    "Tagged the release")
system_checked(
    "git archive --format=zip --prefix=manent-%(VERSION)/ "
    "V%(VERSION)s > releases/manent-%(VERSION).zip" %
    {'VERSION': VERSION},
    "Made a zip file")
system_checked(
    "git archive --format=tar --prefix=manent-%(VERSION)/ "
    "V%(VERSION)s | bzip2 > releases/manent-%(VERSION).tar.bz2" %
    {'VERSION': VERSION},
    "Made a tar.gz file")

