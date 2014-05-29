#!/usr/bin/env python
#
""" Vertica Backup
This script should be run on each node but only one should run the vbr portion, which is set in the config file.
The script will leave a pickle file named with the date in the backup dir which can act as a sentinel file for audits.

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
import os
import subprocess
import sys
import time
import yaml

from directory_metadata import DirectoryMetadata
from epoch import EpochFiles
from object_store.fs import FSStore
from object_store.swift import SwiftStore
from utils import calculate_paths, delete_pickles, LogTime, sizeof_fmt

log = logging.getLogger(__name__)
vbr_bin = '/opt/vertica/bin/vbr.py'


def nagios_exit(exit_status, msg, duration, warn):
    """ Exit in a format fitting for a nagios plugin.
        Backup failure isn't considered critical so only exit good or warning.
    """
    if exit_status != 0:
        print "ERROR: Backup Failed! Exit status %d! %s" % (exit_status, msg)
        sys.exit(1)
    elif duration > warn:
        print "WARNING: " + msg
        sys.exit(1)
    else:
        print "OK: " + msg
        sys.exit(0)


def run_vbr(config):
    """ Run the vbr command according to the values in the configuration.
    """
    output = None
    try:
        vbr_command = [vbr_bin, '--config-file', config['vbr_config'], '--task', 'backup']
        os.environ['LANG'] = 'en_US.UTF-8'  # vbr requires this to be set
        log.info('Running vbr command: %s' % vbr_command)
        output = subprocess.check_output(vbr_command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, cpe:
        nagios_exit(cpe.returncode, "vbr run failed\n%s" % cpe.output, 0, config['warning'])

    # Turns out vbr is not always good about a failed exit code
    for line in output.splitlines():
        if line == 'backup failed!':
            nagios_exit(2, "vbr run failed\n%s" % output, 0, config['warning'])


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) != 2:
        print "Usage: " + argv[0] + " <config file> "
        return 1

    config_file = argv[1]
    config = yaml.load(open(config_file, 'r'))

    # Setup logging
    log_path = os.path.join(config['log_dir'], 'backup_' + datetime.today().strftime('%A') + '.log')
    logging.basicConfig(format='%(asctime)s %(message)s', filename=log_path, level=logging.INFO)

    # log_time is not used here so the timing can be reported to nagios
    start = time.time()
    exit_status = 0

    # Run the vbr backup command - The vbr run is quite fast typically completing in less than a minute
    if config['run_vbr']:
        run_vbr(config)  # If this fails it will sys.exit with an appropriately bad nagios error

    try:
        base_dir, prefix_dir = calculate_paths(config)
        swift_store = SwiftStore(config['swift_key'], config['swift_region'], config['swift_tenant'],
                                 config['swift_url'], config['swift_user'], prefix_dir)
        fs_store = FSStore(base_dir, prefix_dir)
        upload_time = datetime.today()

        epoch_files = EpochFiles(os.path.join(base_dir, prefix_dir), config['snapshot_name'], upload_time)
        epoch_files.archive()

        # Grab the local and swift metadata
        current_metadata = DirectoryMetadata(fs_store, upload_time)
        current_metadata.save(fs_store)
        swift_metadata = DirectoryMetadata(swift_store)

        # Compare the files in the current backup and swift and upload as necessary, then delete as necessary
        with LogTime(log.debug, "Diff operation completed", seconds=True):
            to_add, do_not_del = current_metadata.diff(swift_metadata)

        size_uploaded = 0
        with LogTime(log.info, "Uploaded Completed"):
            for relative_path in to_add:
                size_uploaded += swift_store.upload(relative_path, base_dir)
        log.info("\tUploaded %s in %d items" % (sizeof_fmt(size_uploaded), len(to_add)))

        with LogTime(log.info, "Determining items to delete, retaining %d backups" % config['retain']):
            # Grab the pickle names I want to combine, relying on these being in order by date, newest first
            pickles = swift_store.list_pickles()
            combine_pickles = pickles[:config['retain']]

            # Take metadata in all these pickles combine.
            # It would be good to check that there is no overlap in filenames with different content.
            combined_metadata = DirectoryMetadata()
            for pickle in combine_pickles:
                pickle_metadata = DirectoryMetadata.load_pickle(swift_store, pickle)
                combined_metadata.metadata.update(pickle_metadata.metadata)

            # Do a diff with all that is in swift, anything in swift but not in the combined set can be deleted.
            should_be_empty, to_del = combined_metadata.diff(swift_metadata)
            if len(should_be_empty) != 0:
                exit_status = 1
                log.error(
                    "ERROR: Found files in the %d combined retained backups that were not in swift.\n%s"
                    % (config['retain'], should_be_empty)
                )

        with LogTime(log.info, "Deleted %d items" % len(to_del)):
            for relative_path in to_del:
                swift_store.delete(relative_path)

        # Upload today's metadata pickle, this is done last so its presence an indication the backup is done.
        current_metadata.save(swift_store)

        #Clean up old pickles
        delete_pickles(fs_store)
        delete_pickles(swift_store, config['retain'])

    except:
        log.exception('Unhandled Exception in Backup upload')
        # Move the Epoch files back to their original names so a retry run does not encounter issues with them
        epoch_files.restore()
        exit_status = 1

    # Status message and exit
    stop = time.time()
    duration = (stop - start) / 60
    duration_msg = "Backup completed in %d minutes total. Thresholds, warn %d.|%d" % \
                   (duration, config['warning'], duration)
    log.info(duration_msg)

    nagios_exit(exit_status, duration_msg, duration, config['warning'])


if __name__ == "__main__":
    sys.exit(main())
