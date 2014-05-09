""" A collection of functions to assist with archiving/restoring epoch files.
    Epoch files are the few files which can change from one backup to the next and so must be
    date stamped to differentiate multiple backups from each other.
    Typically only archive_epoch_files and restore_epoch_files are called

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
import logging
import os
import shutil

log = logging.getLogger(__name__)


class EpochFiles(object):
    """ The on disk epoch files and methods to archive/restore them for a given date.
    """

    def __init__(self, backup_dir, snapshot_name, date):
        self.date_str = date.strftime("%Y_%m_%d_%H%M")
        self.epoch_files = self._get_epoch_files(backup_dir, snapshot_name)

    @staticmethod
    def _get_epoch_files(backup_dir, snapshot_name):
        files = [
            os.path.join(backup_dir, snapshot_name + '.txt'),
            os.path.join(backup_dir, snapshot_name + '.info'),
            glob(os.path.join(backup_dir, 'var/vertica/catalog/*/v_*_catalog/Snapshots'))[0] + '/catalog.ctlg',
        ]
        return files

    @staticmethod
    def _move_file(from_path, to_path):
        """ Move a file on the local filesystem, logging an error if the from file does not exist.
        """
        if os.path.exists(from_path):
            shutil.move(from_path, to_path)
        else:
            log.error('File %s not found when attempting to move to %s' % (from_path, to_path))

    def archive(self):
        """ Copy epoch files to their date stamped names
        """
        for path in self.epoch_files:
            self._move_file(path, "%s_%s" % (path, self.date_str))

    def restore(self):
        """ Copy epoch files from their date stamped names to their standard names
        """
        for path in self.epoch_files:
            self._move_file("%s_%s" % (path, self.date_str), path)
