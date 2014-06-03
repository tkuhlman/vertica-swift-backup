<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Vertica Swift Backup](#vertica-swift-backup)
  - [Goals](#goals)
  - [Installation and Configuration](#installation-and-configuration)
  - [Restores](#restores)
  - [Tests](#tests)
    - [Vagrant test cluster](#vagrant-test-cluster)
  - [Architecture](#architecture)
    - [Components](#components)
      - [DirectoryMetadata](#directorymetadata)
      - [ObjectStore](#objectstore)
    - [A note about directory paths](#a-note-about-directory-paths)
  - [Future work](#future-work)
    - [Encryption](#encryption)
    - [Multi-Threading Swift](#multi-threading-swift)
    - [Smart backup retries](#smart-backup-retries)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Vertica Swift Backup
[![Build Status](https://travis-ci.org/tkuhlman/vertica-swift-backup.svg?branch=master)](https://travis-ci.org/tkuhlman/vertica-swift-backup)

Backup/Restore [Vertica](http://www.vertica.com/) to/from OpenStack [Swift](https://wiki.openstack.org/wiki/Swift)

## Goals
- Nightly backups of the Vertica database sent to swift. The upload should take less than a day even for large databases.
- Backups must be incrementally uploaded to conserve bandwidth used for each upload and space usage on swift.
- Retention of historical backups (configurable)

## Installation and Configuration
There is a chef recipe that will install and setup nodes as part of this [cookbook](https://github.com/hpcloud-mon/cookbooks-vertica/blob/master/recipes/backup.rb). This cookbook is used in the test Vagrant setup detailed below.

To manually setup install with `pip install vertica-swift-backup` and configure as below.

To configure a Vertica cluster for backup, on each node setup:
- A backup directory. This should be on the same device as the Vertica data directory so that the hard link backup will work correctly.
- A vbr configuration set to run a [hard link local](https://my.vertica.com/docs/7.0.x/HTML/index.htm#Authoring/AdministratorsGuide/BackupRestore/CreatingHardLinkLocalBackups.htm%3FTocPath%3DAdministrator's%20Guide%7CBacking%20Up%20and%20Restoring%20the%20Database%7C_____10) backup.
- A configuration for this backup tool, sample in `/usr/local/share/vertica-swift-backup/backup.yaml-example`. Only one node should be configured to run vbr.
  In the specified swift account a container with the same name as the domain of the Vertica nodes should be created.
- A cron job that runs the backup script, simply run the command with the config as an argument, ie `vertica_backup backup_config.yaml`.i
  The vbr step only runs on one node so that node should be set to start before the others. A link local backup
  is typically quite fast so the gap need not be too large.
- Optionally this can be setup so the output goes to a monitoring system. The backup script exits with a message and
  exit code compatible with nagios plugins so it can be used to report status to many monitoring tools. Additionally
  an external auditing tool can look for the dated pickle created as the last step of the backup to verify completion.

If no previous backup DirectoryMetadata is found a full backup will be done otherwise an incremental.

## Restores
Like backups restores have both a slow swift component and a fast vbr component. Unlike backups the slow part comes
first. Any of the retained backups can be restored simply by choosing the correct pickle and corresponding epoch
files.

The download from swift part of the restore is done via the restore_download script which is part of the vertica_backup code.
After the backup is downloaded on each node of a cluster the rest of the restore is handled by vbr. Coordinating the download
and vbr run across multiple nodes can be a bit tricky so I have included fabric scripts found in the restore directory of the
git repo and installed in /usr/local/share/vertica-swift-backup. The fabric scripts will rearrange the
directories on a test cluster and run vbr after the download finishes and do so in such a way as to not destroy
existing data on that cluster.

## Tests
The unit tests reside in the top level tests directory and can be run with nose.

### Vagrant test cluster
A vagrantfile and appropriate chef configuration are available in this repository so a 3 node vertica cluster can be setup and used for test
backup/restore. To run install [Vagrant](http://www.vagrantup.com/), [Berkshelf](http://berkshelf.com/) and the vagrant berkshelf plugin

Vertica must be downloaded from the [Vertica site](https://my.vertica.com/). Download these packages and place in the root of this repository.
- vertica_7.0.1-0_amd64.deb
- vertica-R-lang_7.0.1_amd64.deb

The vertica::console recipe is not enabled by default but if it is added this package is also needed.
- vertica-console_7.0.1-0_amd64.deb

Create `data_bags/vertica/backup_credentials.json` with your swift credentials following the pattern in `data_bags/vertica/backup_credentials.json-template`
Then simply run `vagrant up`

## Architecture
There are a number of key points about how swift and vertica work that explain the architecture of this script.

- The included Vertica backup utility vbr.py is used to create a local backup of Vertica. This utility is written
  by the Vertica team and properly creates a snapshot and local backup using hard links. Since hard links are used
  the backup is of a small size. All files sent to swift work from this local backup. The command must only be run
  on a single node, which node is specified in the backup configuration. Once vbr is run each node in the cluster
  can send its data to swift asynchronously.
- Vertica does not modify data files but rather deletes them and creates a new uniquely named file. This is the essential
  bit of knowledge needed to allow an incremental backup to work as well as retaining multiple backups in swift.
  - An incremental backup takes the set of data files one day and the next just adds the new set, no other changes
    are needed.
  - Previous days backups can be retained by delaying the delete of files no longer in the primary set and retaining
    the appropriate epoch information. This epoch data is stored in a few files which are simply uploaded to
    swift with an date in the filename.

### Components
#### DirectoryMetadata
This class contains the metadata information for files in any given snapshot. It is built using an ObjectStore.
This is intended to be persisted on disk as a pickle and uploaded after the other files.
In this way it acts as a sentinel file indicating the backup is complete.
Also two different instances of the class can be compared to determine files to upload and/or
delete. This ability to compare the two sets along with the files being persisted to swift enables incremental backups,
incremental downloads as well as delayed cleanup of backups.

#### ObjectStore
ObjectStore is an abstract class which is implemented by SwiftStore and FSStore. These objects are used for all storage
operations most notably the collecting of metadata for creation DirectoryMetadata objects and the download/upload as
needed for backup/restore.

### A note about directory paths
Any on disk backups have a *base_dir* where backups reside and a prefix_dir where the specific backup being worked
with resides. The first element of the prefix path is the vertica node name, the second the snapshot name.
The prefix path is specified this way to match the structure used by vbr.
The scripts only deal with 1 snapshot at a time, keeping the prefix_path separate from the
base_dir allows multiple snapshots to exist.

Swift has no base_dir, but rather a container name which is the domain name and hostname joined by a '_'.
The prefix_dir works the same as in the disk object store.

The pickles are stored at the base_dir/container root where the prefix_path starts also. These are not part of the normal backup,
they must explicitly be uploaded/deleted as needed.

## Future work
### Encryption
In the future encrypting backups on swift may be desired. I envision doing this with a public/private key combo with the
public key residing on the clients and the private being used for restores. Backups are unchanged other than encrypting
the file just before sending as they compare two different days of the local disk.

### Multi-Threading Swift
In my environment backups are completing in 4-8 hours, which is sufficient, if they slow multi-threading the swift
uploads/deletes could speed things up.

### Smart backup retries
Occasionally a backup fails because of a disk, network or swift error. In those cases I simply rerun the backup job, however
this creates the situation where there are two backups for one day and the retained backups in swift represent one less day
of backups on that node. Better would be for the 2nd backup on the day to discover it is not first and replace the earlier
backup's metadata/epoch files so there still remains only 1 backup per day.
