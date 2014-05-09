A set of fabric tasks for doing a vertica database restore.
The actual download is done using the vertica_restore_download program which is part of the
vertica_backup scripts and is assumed to be installed on each node.
The restore is done using the vbr utility.

The tasks here orchestrate the various steps across multiple boxes.
Also the scripts enable doing test restores to a test cluster by moving an old db
out of the way and making the necessary modifications to config.

# Restoring a db

** NOTE: The script has a number of hard coded paths and dbnames in it that need fixing. **

A few assumptions about the cluster you are restoring to include:
- The nodes in that cluster must already be setup, this config is how the script discovers all nodes.
- The backup configuration and scripts must be setup on each node so the download from swift can occur.
- The cluster must be the same size as the backup.

The script requires that [fabric](http://www.fabfile.org/) is installed and ready for use.
To run a restore from this directory (or specifically pointing to the fabfile.py) run
`fab vertica.restore -H node.fqdn` where node.fqdn is a fqdn of one of the nodes to restore the db to.
