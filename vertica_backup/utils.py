"""
Copyright 2014 Hewlett-Packard Development Company, L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
and associated documentation files (the "Software"), to deal in the Software without restriction, 
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, 
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or 
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
PURPOSE AND NONINFRINGEMENT.

IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF 
OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from glob import glob
import os
import time


class LogTime(object):
    """ Used by the python 'with' syntax this will time the operation and log the details to the log
    """
    def __init__(self, log, msg, seconds=False):
        """ Log is function that can be called for logging
            msg is a string formatted message
            msg_details is a tuple that will be applied to message. The idea being to specify varibles that will
            be updated during executiion and made part of the final message
        """
        self.log = log
        self.msg = msg
        self.seconds = seconds
        self.start = None
        self.end = None

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, atype, value, traceback):
        self.end = time.time()
        if self.seconds:
            self.log(self.msg + " in %d seconds" % (self.end - self.start))
        else:
            self.log(self.msg + " in %d minutes" % ((self.end - self.start)/60))


def calculate_paths(config, v_node_name=None):
    """ Returns the base_dir and prefix_dir given the config and v_node_name
        If the v_node_name is None it pulls the info from the local drive.
    """
    if v_node_name is None:
        v_node_name = glob(os.path.join(config['backup_dir'], 'v_' + config['dbname'] + '_node*'))[0].split('/')[-1]
    prefix_dir = os.path.join(v_node_name, config['snapshot_name'])
    return config['backup_dir'], prefix_dir


def choose_one(alist, msg):
    """ Prompt for a choice of one item from the list, return that item
    """
    if len(alist) == 0:
        return None
    elif len(alist) == 1:
        return alist[0]

    print(msg)
    for index, value in enumerate(alist):
        print('%d: %s' % (index, value))

    choosen = raw_input("\tPlease specify the index: ")
    return alist[int(choosen)]


def delete_pickles(store, keep=1):
    """ Remove pickles in the store, keeping the specified number of the newest pickles.
    """
    pickles = store.list_pickles()
    for pickle in pickles[keep:]:
        store.delete(pickle)


def sizeof_fmt(num):
    """ Yanked from http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

