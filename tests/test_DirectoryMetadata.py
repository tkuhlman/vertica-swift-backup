""" Tests converting the DirectoryMetadata class
"""

import time

from vertica_backup.directory_metadata import DirectoryMetadata
from vertica_backup.object_store.fs import FSStore

test_objects = {}


def setup():
    """ Load objects from the local drive. This assumes the FS object store works right."""
    cwd = FSStore('./', '')
    local = DirectoryMetadata(cwd)
    test_objects['local'] = local
    local_copy = DirectoryMetadata(cwd)
    test_objects['local_copy'] = local_copy

    usr_share_store = FSStore('/usr/share', '')
    usr_share = DirectoryMetadata(usr_share_store)
    test_objects['usr_share'] = usr_share

    usr_share_doc_store = FSStore('/usr/share', 'doc')
    usr_share_doc = DirectoryMetadata(usr_share_doc_store)
    test_objects['usr_share_doc'] = usr_share_doc


def test_diff_equality():
    assert test_objects['local'].diff(test_objects['local_copy']) == (set(), set())


def test_diff_adds():
    empty = DirectoryMetadata()
    assert test_objects['local'].diff(empty) == (set(test_objects['local'].metadata.keys()), set())


def test_diff_dels():
    empty = DirectoryMetadata()
    assert empty.diff(test_objects['local']) == (set(), set(test_objects['local'].metadata.keys()))


def test_diff():
    """ A more complete test of the diff function.
    """
    usr_share = test_objects['usr_share']
    usr_share_doc = test_objects['usr_share_doc']
    additions = set()
    deletions = set()

    for path in usr_share.metadata.keys():
        if path not in usr_share_doc.metadata.keys():
            additions.add(path)

    bin_store = FSStore('/', 'bin')
    bin = DirectoryMetadata(bin_store)
    usr_share_doc.metadata.update(bin.metadata)
    for path in usr_share_doc.metadata.keys():
        if path.startswith('bin'):
            deletions.add(path)

    start = time.time()
    result = usr_share.diff(usr_share_doc)
    end = time.time()
    print 'Test diff ran in %f seconds' % (end - start)
    assert result == (additions, deletions)
