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
import json
import logging
import os
import socket
import tempfile

import swiftclient

from ..directory_metadata import FileMetadata
from . import ObjectStore

log = logging.getLogger(__name__)


class SwiftException(Exception):
    pass


class SwiftStore(ObjectStore):
    """ Wraps swiftclient with a number of methods tailored for use by the vertica backup.

        Sets the swift container to the domain and puts all files in a subdir for the host.
    """

    def __init__(self, key, region, tenant, url, user, prefix, domain=None, hostname=None, vnode=None):
        """ Takes the config object from the backup.py.
            If the domain is specified either the hostname or vnode should be.
            If vnode is specified and hostname isn't the hostname will be discovered from what is in swift. This only
            works if existing backups are in swift and is useful primarily for restore jobs.
        """
        self.key = key
        self.region = region
        self.tenant = tenant
        self.url = url
        self.user = user
        self.prefix = prefix

        self.conn = self._connect_swift()

        if domain is None:
            hostname, domain = socket.getfqdn().split('.', 1)
        elif hostname is None:
            if vnode is None:
                raise SwiftException(
                    'Error creating SwiftStore: If domain is defined then either hostname or vnode must also be.'
                )
            hostname = self._get_hostname_from_vnode(domain, vnode)

        self.container = "%s_%s" % (domain, hostname)
        log.debug("Using container %s" % self.container)
        if len(self.conn.get_account(prefix=self.container)[1]) == 0:
            log.info("Creating container %s" % self.container)
            self.conn.put_container(self.container)

    def _connect_swift(self):
        """ Start up a swift connection
        """
        return swiftclient.client.Connection(self.url, self.user, self.key, os_options={"region_name": self.region},
                                             tenant_name=self.tenant, auth_version=2)

    def _download(self, swift_path, local_path):
        """ Download the file from swift_path to local_path.
        """
        log.debug('Download from swift %s' % swift_path)
        with open(local_path, 'wb') as local_file:
            try:
                local_file.write(self.conn.get_object(self.container, swift_path)[1])
            except swiftclient.ClientException, ex:
                if ex.http_status == 404:
                    log.error('Failed downloading %s from swift, file does not exist.' % swift_path)
                else:
                    log.error('Error downloading from swift %s. Details:\n%s' % (swift_path, ex.msg))

    def _get_hostname_from_vnode(self, domain, vnode):
        """ Discover a hostname by looking in swift for the hostname associated with a particular vertica node name.
            This assumes swift has an existing backup and there is a 1 to 1 mapping of vnode name to hostname.
        """
        for container in self.conn.get_account(prefix=domain + '_')[1]:
            for node_name in self.conn.get_object(container['name'], '', query_string='delimiter=/')[1].splitlines():
                if vnode == node_name.strip('/'):
                    return container['name'].split('_', 1)[1]

        raise SwiftException('No hostname could be determined from swift for the vnode %s, domain %s' % (vnode, domain))

    @staticmethod
    def _normalize_metadata(raw_metadata):
        """ Cleanup the metadata returned from swift

            Convert the date to a datetime object.
            Skip any directories, I am looking at files only.
            Turn into a metadata dictionary with key the path and the value a FileMetaData object
        """
        clean = {}
        for old_metadata in raw_metadata:
            if old_metadata['content_type'] == 'application/directory':
                continue
            path = old_metadata['name']
            mtime = datetime.strptime(old_metadata['last_modified'], '%Y-%m-%dT%H:%M:%S.%f')
            file_metadata = FileMetadata(path, old_metadata['bytes'], mtime, old_metadata['hash'])
            clean[path] = file_metadata
        return clean

    def _upload(self, local_path, swift_path):
        """ Upload a file from the local_path to swift.
        """
        log.debug('Upload to swift %s' % local_path)
        with open(local_path, 'rb') as object_file:
            try:
                self.conn.put_object(self.container, swift_path, object_file)
            except swiftclient.ClientException, ex:
                log.error('Error uploading to swift %s, retrying. Details:\n%s' % (local_path, ex.msg))
                self.conn = self._connect_swift()
                self.conn.put_object(self.container, swift_path, object_file)

    def delete(self, swift_path):
        """ Delete an object """
        log.debug('Delete from swift %s' % swift_path)
        try:
            self.conn.delete_object(self.container, swift_path)
        except swiftclient.ClientException, ex:
            if ex.http_status == 404:
                log.debug('Failed deleting %s from swift, file does not exist.' % swift_path)
            else:
                log.error('Error deleting from swift %s. Details:\n%s' % (swift_path, ex.msg))

    def download(self, relative_path, local_path):
        """ Download the object from swift and store in local_path
            Return the size of the object if successful
        """
        file_path = os.path.join(local_path, relative_path)
        p_dir = os.path.dirname(file_path)
        if not os.path.exists(p_dir):
            os.makedirs(p_dir)

        self._download(relative_path, file_path)
        return os.path.getsize(file_path)

    def get_metadata(self):
        """ Return the metadata parsed from the json response for all files in the prefix path.
        """
        #Setting format=json is the special sauce to get back metadata rather than just names
        #setting prefix= allows getting subsets of files
        # The response is an object with two items the first is the headers the 2nd the json body
        query_string = 'prefix=%s&format=json' % self.prefix

        metadata = {}

        more_results = True
        marker = ''
        while more_results:  # Loop getting all results, only 10000 will be returned in one request.
            try:
                swift_files = json.loads(self.conn.get_object(self.container, '', query_string=query_string + marker)[1])
            except swiftclient.ClientException, ex:
                log.error('Error retrieving metadata from swift, retrying. Details:\n%s' % ex.msg)
                self.conn = self._connect_swift()
                swift_files = json.loads(self.conn.get_object(self.container, '', query_string=query_string + marker)[1])
            metadata.update(self._normalize_metadata(swift_files))
            if len(swift_files) < 10000:
                more_results = False
            else:
                marker = '&marker=' + swift_files[-1]['name']

        return metadata

    def list_dir(self, path='/'):
        """ A non-recursive listing of a directory in swift.
            Returns a list of item names.
        """
        if len(path) == 0 or path[-1] != '/':
            path += '/'

        # By specifying the delimiter this list is not recursive
        if path == '/':  # skip the prefix to list the root
            query_string = 'delimiter=/'
        else:
            query_string = 'prefix=%s&delimiter=/' % path

        return self.conn.get_object(self.container, '', query_string=query_string)[1].splitlines()

    @contextmanager
    def open(self, path, flags):
        """ Open a file at path in the object store with the appropriate flags
        """
        # Download the file to a temporary location
        tmp_fd, tmp_path = tempfile.mkstemp()
        os.close(tmp_fd)  # I will be opening/closing the file as needed, leaving this open complicates it

        if (flags.find('r') != -1) or (flags.find('a') != -1):  # Used for reading/appending
            self._download(path, tmp_path)

        yield_file = open(tmp_path, flags)
        yield yield_file
        yield_file.close()

        if (flags.find('w') != -1) or (flags.find('a') != 1):  # It was used for writing, upload the changed file
            self._upload(tmp_path, path)

        # Cleanup the temporary file descriptor and path
        os.remove(tmp_path)

    def upload(self, relative_path, base_dir):
        """ Upload a file from base_dir/relative_path to swift. Returns the size of the file if successful."""
        file_path = os.path.join(base_dir, relative_path)
        self._upload(file_path, relative_path)
        return os.path.getsize(file_path)

