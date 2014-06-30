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
import fnmatch
import re


class ObjectStore(object):
    """ Abstract base class for Object stores which hold Vertica backups and DirectoryMetadata pickles
    """
    def delete(self, path):
        """ Remove path from the ObjectStore
        """
        raise NotImplementedError

    def download(self, relative_path, fs_path):
        """ Download from the relative path in the ObjectStore to the local filesystem path.
            Return the size of the object if successful
        """
        raise NotImplementedError

    def get_metadata(self):
        """ Returns a dictionary with key of path and values of FileMetadata objects
            The metadata object is build from the prefix path of the object store.
        """
        raise NotImplementedError

    def list_dir(self, path='/'):
        raise NotImplementedError

    def list_pickles(self):
        """ Return a list of all pickles found in the Object store, reverse sorted by filename.
            Since the pickles are named by date the reverse sorting ends up with the newest first and the oldest last.
        """
        pickle_list = []
        root_list = self.list_dir()
        if root_list is not None:
            pickle_re = re.compile(fnmatch.translate('*.pickle'))
            for filename in root_list:
                if pickle_re.match(filename) is not None:
                    pickle_list.append(filename)

            pickle_list.sort(reverse=True)
        return pickle_list

    def open(self, path, flags):
        """ A context manager used with python 'with' to open a file in the ObjectStore.
        """
        raise NotImplementedError

    def upload(self, relative_path, base_dir):
        """ Upload the file from os.path.join(base_dir, relative_path) on the local os to relative_path in the store.
            Returns the file size if successful
        """
        raise NotImplementedError
