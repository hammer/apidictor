#!/usr/bin/env python
"""
Grab Hive file size information into a tree.

TODO: persistent store for gathered information and time of last scrape
TODO: only search those directories with an update time > last scrape
TODO: treat ds= folders differently (check size change each day)
TODO: get file type information from HDFS so i don't have to du on files

@author hammer
"""
import time
import threading
import Queue
import subprocess
import logging

num_threads = 20

class process_inode(threading.Thread):
  def __init__(self, thread_id, node_queue, tree):
    threading.Thread.__init__(self)
    self.thread_id = thread_id
    self.node_queue = node_queue
    self.tree = tree
    self.num_retries = 0
    # some constants
    self.MAX_RETRIES = 3
    self.RETRY_INTERVAL_SECONDS = 2

  def run(self):
    while True:
      # grab an inode from the queue
      # try a few times in case there's another thread working (this should be better)
      try:
        inode = self.node_queue.get_nowait()
      except Queue.Empty:
        self.num_retries += 1
        if self.num_retries > self.MAX_RETRIES:
          log.info("Thread %s: Empty queue, exiting." % self.thread_id)
          break
        else:
          log.info("Thread %s: Empty queue, num_retries now %s" % (self.thread_id, self.num_retries))
          time.sleep(self.RETRY_INTERVAL_SECONDS)
          continue

      # process the node
      log.info("Thread %s: Processing inode %s" % (self.thread_id, inode))
      children = get_children(inode)

      # children of our inode
      self.tree[inode]['children'] = children.keys()
      # more inodes to munch
      for child in children.keys():
        self.node_queue.put_nowait(child)
      log.info("Current queue length: %s" % node_queue.qsize())
      # record sizes here
      for child, size in children.iteritems():
        self.tree[child] = {'children': [], 'size': size}


def get_children(parent):
  """
  parse results of an dfs du command into a dict

  @author: hammer
  """
  ps = subprocess.Popen(du_cmd + [parent], stdout = subprocess.PIPE)
  children = dict([(line.strip().split('\t')[0].split(':9000')[1], line.strip().split('\t')[1]) for line in ps.stdout.readlines() if line.startswith('hdfs://')])
  # get rid of the parent (i.e. this is a file)
  children.pop(parent, None)
  return children


def print_tree(tree, root):
  """
  print a tree via level-order/breadth-first traversal

  @author: hammer
  """
  queue = [root]
  while len(queue) > 0:
    inode = queue.pop()
    print "%s: %s" % (inode, tree[inode]['size'])
    queue += tree[inode]['children']


if __name__ == "__main__":
  # log config
  log = logging.getLogger("apidictor_collector")
  log_loc = "" # logfile location goes here
  log.addHandler(logging.FileHandler(log_loc))
  log.setLevel(logging.INFO)

  # command to list files and sizes in an hdfs directory
  hadoop_bin = "" # hadoop binary location goes here
  du_cmd = [hadoop_bin, 'dfs', '-du']

  # where we'll start exploring
  path_to_hive = '' # path to hive in your hdfs installation goes here
  root = path_to_hive
  node_queue = Queue.Queue()
  node_queue.put_nowait(root)

  # list of threads
  thread_list = []

  # where we'll store our results
  tree = {root: {'children': [], 'size': 0}}

  # prime the threads
  for thread_id in range(num_threads):
    t = process_inode(thread_id, node_queue, tree)
    thread_list.append(t)

  # off we go!
  log.info("Starting %s threads..." % num_threads)
  for t in thread_list:
    t.start()

  # wait until they finish
  for t in thread_list:
    t.join()

  # what do we have?
  print_tree(tree, root)



