#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import logging

class Listener:
  def __init__(self, prefix):
    self.prefix = prefix
  def is_active_for(self, name):
    return name.startswith(self.prefix)
  def notify(self, name, value):
    pass

class CallbackListener(Listener):
  def __init__(self, prefix, callback):
    Listener.__init__(self, prefix)
    self.callback = callback
  def notify(self, name, value):
    self.callback(name, value)

class DummyReporter:
  """Placeholder for a reporter that has a reporter interface but does nothing.
  Useful to substitute for None, in places where a reporter might not be
  available, to avoid code like "if reporter is not None: reporter.increment".
  """
  def set(self, value):
    pass
  def increment(self, value):
    pass
  def append(self, value):
    pass
class DummyReportManager:
  """Placeholder for a report manager that does nothing."""
  def find_reporter(self, name, initial):
    return DummyReporter()
  def set(self, name, value):
    pass
  def increment(self, name, value):
    pass
  def append(self, name, value):
    pass

class Reporter:
  def __init__(self, name, initial):
    self.name = name
    self.value = initial
    self.listeners = []
  def add_listener(self, listener):
    if listener.is_active_for(self.name):
      self.listeners.append(listener)
  def notify_listeners(self):
    for listener in self.listeners:
      listener.notify(self.name, self.value)
  def set(self, value):
    """Assumes that value is a scalar"""
    self.value = value
    self.notify_listeners()
  def increment(self, value):
    """Assumes that value is a number"""
    self.value += value
    self.notify_listeners()
  def append(self, value):
    """Assumes that value is a list"""
    self.value.append(value)
    self.notify_listeners()

class ReportManager:
  def __init__(self):
    self.reporters = {}
    self.listeners = []
  def write_report(self, file):
    for name in sorted(self.reporters.keys()):
      reporter = self.reporters[name]
      if type(reporter.value) == unicode:
        file.write("%s: %s\n" % (name, reporter.value.encode('utf8')))
      else:
        file.write("%s: %s\n" % (name, str(reporter.value)))
  def set(self, name, value):
    self.find_reporter(name, None).set(value)
  def increment(self, name, value):
    self.find_reporter(name, 0).increment(value)
  def append(self, name, value):
    self.find_reporter(name, []).append(value)
  def find_reporter(self, name, initial):
    if not self.reporters.has_key(name):
      logging.debug("Creating new reporter %s" % name)
      reporter = Reporter(name, initial)
      for listener in self.listeners:
        reporter.add_listener(listener)
      self.reporters[name] = reporter
    return self.reporters[name]
  def add_listener(self, listener):
    self.listeners.append(listener)
    for reporter in self.reporters.itervalues():
      reporter.add_listener(listener)

