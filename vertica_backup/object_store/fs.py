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

from contextlib import contextmanager
from datetime import datetime
import hashlib
import logging
import os
import shutil

from ..directory_metadata import FileMetadata, DirectoryMetadata
from . import ObjectStore

log = logging.getLogger(__name__)


class FSStore(ObjectStore):
    """ An object store part of a locally mounted filesystem
    """
    def __init__(self, base_dir, prefix):
        if base_dir[-1] != '/':  # Make sure there is a trailing / so the relative path does not begin with one
            base_dir += '/'
        self.base_dir = base_dir
        self.prefix_dir = os.path.join(base_dir, prefix)

    def _get_full_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.join(self.base_dir, path)

    def delete(self, path):
        """ Remove the path from the file store
        """
        full_path = self._get_full_path(path)
        if os.path.exists(full_path):
            os.remove(full_path)

    def download(self, relative_path, fs_path):
        """ Copy from the relative path in the file store to the fs_path
            Return the size if successful
        """
        shutil.copy(self._get_full_path(relative_path), fs_path)
        return os.path.getsize(fs_path)

    def get_metadata(self):
        """ Read the disk directory location creating a DirectoryMetadata object.
            If a previous DirectoryMetadata object cna be found the md5 sums for old files will be copied from there, this
            speeds up the process of collecting metadata significantly.
        """
        previous = DirectoryMetadata.load_pickle(self)
        metadata = {}

        for dirpath, dirnames, filenames in os.walk(self.prefix_dir):
            for fname in filenames:
                path = os.path.join(dirpath, fname)
                relative_path = path.split(self.base_dir, 1)[1]
                try:
                    stats = os.stat(path)
                except OSError:
                    log.exception('Error stating a file on disk while building up metadata, skipping file %s' % path)
                    continue
                swift_bytes = stats.st_size
                mtime = datetime.utcfromtimestamp(stats.st_mtime)
                if (previous is not None) and (relative_path in previous.metadata) and\
                        (previous.metadata[relative_path].bytes == swift_bytes):
                    swift_hash = previous.metadata[relative_path].hash
                else:
                    try:
                        with open(path, 'rb') as afile:
                            md5_hash = hashlib.md5()
                            md5_hash.update(afile.read())
                            swift_hash = md5_hash.hexdigest()
                    except OSError:
                        log.exception('Error reading a file to create the md5 while building up metadata, skipping file %s' % path)
                        continue

                metadata[relative_path] = FileMetadata(relative_path, swift_bytes, mtime, swift_hash)

        return metadata

    def list_dir(self, path='/'):
        return os.listdir(self._get_full_path(path))

    @contextmanager
    def open(self, relative_path, flags):
        """ Open a file at the base of the object store + relative path with the appropriate flags
        """
        file_obj = open(self._get_full_path(relative_path), flags)
        yield file_obj
        file_obj.close()

    def upload(self, relative_path, base_dir):
        """ Copy the file from base_dir/relative_path to the object store relative_path
            Return the size if successful
        """
        full_path = self._get_full_path(relative_path)
        shutil.copy(os.path.join(base_dir, relative_path), full_path)
        return os.path.getsize(full_path)
