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

from datetime import datetime
import logging
import pickle

from utils import LogTime

log = logging.getLogger(__name__)


class FileMetadata(object):
    """ Simple class for storing file metadata. """
    __slots__ = ('path', 'bytes', 'mtime', 'hash')

    def __init__(self, path, f_bytes, mtime, f_hash):
        self.path = path
        self.bytes = f_bytes
        self.hash = f_hash
        self.mtime = mtime

    def __eq__(self, other):
        """ Don't compare the mtime as that is specific to the origin filesystem. """
        # Not the prettiest but twice the speed of other methods I tested, A million equals in 0.707575 seconds
        if (self.path == other.path) and (self.bytes == other.bytes) and (self.hash == other.hash):
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class DirectoryMetadata(object):
    """ An object representing the metadata for a collection of files. Implements a few comparison functions.
    """

    def __init__(self, store=None, date=datetime.today()):
        """ Initialization, takes a store object and prefix to populate the object.
              Sets self.metadata a dictionary with path as key and value a FileMetadata object
        """
        self.date = date
        if store is None:
            metadata = {}
        else:
            with LogTime(log.info, "Collected %s metadata" % store.__class__.__name__):
                metadata = store.get_metadata()
            log.info("\tCollected %d total files" % len(metadata))

        self.metadata = metadata  # A dictionary with path as key and value a FileMetadata object

    def diff(self, other):
        """ Compare with another DirectoryMetadata object
            returns two sets of filenames
                the first set is in self but not other or not the same in other
                the second set is not in self but is in other
        """
        additions = set()

        other_keys = set(other.metadata.keys())

        for path in self.metadata.iterkeys():
            if path not in other_keys:
                additions.add(path)
                continue

            other_keys.remove(path)
            if self.metadata[path] != other.metadata[path]:
                additions.add(path)
                # the precence of such a file most likely indicates an error during upload on a previous run
                log.warning('%s is in both DirectoryMetadata objects but the files differ.' % path)

        return additions, other_keys

    def save(self, store):
        """ Save to a pickle with today's date as the filename.
        """
        pickle_name = self.date.strftime("%Y_%m_%d_%H%M") + '.pickle'
        with store.open(pickle_name, 'w') as pickle_file:
            pickle.dump(self, pickle_file, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load_pickle(store, pickle_name=None):
        """ Load the pickle name specified from the ObjectStore. If no name, load the newest.
            returns None if nothing is found.
        """
        if pickle_name is None:
            pickles = store.list_pickles()
            try:
                pickle_name = pickles[0]
            except IndexError:
                return None

        with store.open(pickle_name, 'r') as pickle_file:
            metadata = pickle.load(pickle_file)

        if isinstance(metadata, DirectoryMetadata):
            return metadata
        else:
            return None
