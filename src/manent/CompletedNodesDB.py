#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import logging

# The database of completed nodes. This database is held in memory (the
# completed nodes are highly write/delete intensive but small). It is read in
# upon application startup and written out in every commit.
class CompletedNodesDB:
  def __init__(self, db_manager, txn_manager):
    self.db_manager = db_manager
    # TODO(gsasha): set a hook on pre-commit of txn_manager to save
    self.txn_manager = txn_manager
    self.completed_nodes_mem = {}
    self.completed_nodes_db = self.db_manager.get_database(
        "storage.db", "completed_nodes_db", self.txn_manager)
    self.precommit_hook = self.save
    self.txn_manager.add_precommit_hook(self.precommit_hook)
  def close(self):
    self.completed_nodes_db.close()
    self.txn_manager.remove_precommit_hook(self.precommit_hook)
  def load(self):
    """Load the data from the db into memory"""
    self.completed_nodes_mem.clear()
    for key, value in self.completed_nodes_db.iteritems():
      self.completed_nodes_mem[key] = value
  def save(self):
    """Save the data from the db into memory"""
    self.completed_nodes_db.truncate()
    for key, value in self.completed_nodes_mem.iteritems():
      self.completed_nodes_db[key] = value
  def has_key(self, key):
    return self.completed_nodes_mem.has_key(key)
  def __setitem__(self, key, value):
    self.completed_nodes_mem[key] = value
  def __getitem__(self, key):
    return self.completed_nodes_mem[key]
  def __delitem__(self, key):
    del self.completed_nodes_mem[key]

