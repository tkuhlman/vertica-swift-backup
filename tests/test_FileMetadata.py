""" Tests converting the FileMetadata class
"""
from datetime import datetime, timedelta
import time
from vertica_backup.directory_metadata import FileMetadata


def test_equality():
    now = datetime.now()
    one = FileMetadata('./', 5, now, '23490283')
    two = FileMetadata('./', 5, now, '23490283')
    assert one == two


def test_inequality():
    now = datetime.now()
    one = FileMetadata('./', 5, now, '23490283')
    files = [
        FileMetadata('./different', 5, now, '23490283'),
        FileMetadata('./', 6, now + timedelta(minutes=2), '23490283'),
        FileMetadata('./', 5, now, '234ksld83'),
        FileMetadata('./different', 6, now + timedelta(minutes=2), '234ksld83')
    ]
    for two in files:
        yield one.__ne__, two


def test_speed():
    now = datetime.now()
    one = FileMetadata('./', 5, now, '23490283')
    two = FileMetadata('./', 5, now, '23490283')
    start = time.time()
    for x in range(1000000):
        one == two
    end = time.time()
    print "A million FileMetadata compares took %f" % (end - start)
