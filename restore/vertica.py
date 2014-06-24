""" Restore Vertica from Swift backups - see README.md

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
import os
import socket
import tempfile

from fabric.api import *
from fabric.colors import *

# todo setup detection of backup_dir, data_dir and catalog_dir from the the config files and remove references to /var/vertica, update readme


@task
@runs_once
def restore(dbname=None, restore_domain=None):
    """ The master task that calls all the sub tasks walking through the entire process from download to restore to restoration of the previous db.
        Run this with the name of one node in the run list, other nodes will be discovered from there.
    """
    env.abort_on_prompts = False
    env.warn_only = False
    env.sudo_prefix = env.sudo_prefix.replace('sudo ', 'sudo -i ')  # Proper users with sudo see https://github.com/fabric/fabric/issues/503

    # Check that only one box is in the hosts, the others in the cluster discovered from here
    if len(env.hosts) != 1:
        abort('When running vertica.restore only 1 host should be in the host list, this host will run the vbr commands.')

    with settings(hide('running', 'output')):
        current_domain = run('hostname -d').strip()
    if dbname is None:
        dbname = prompt('Which db should be restored?')
    if restore_domain is None:
        restore_domain = prompt('Which domain should be restored?')

    # Discover details of the cluster
    primary_node = env.hosts[0]
    cluster_nodes = get_cluster_nodes(dbname)
# todo the old code assumes hostnames with -cluster doesn't work with ips or other hostnames, the new assumes only 1 interface per node
    env.hosts = cluster_nodes.values()
#    nodes = {}  # 'fqdn':'v_node name'
#    for vnode in cluster_nodes.iterkeys():
#        cluster_fqdn = socket.gethostbyaddr(cluster_nodes[vnode])[0]
#        nodes[cluster_fqdn.replace('-cluster', '')] = vnode  # This relies on a specific cluster ip naming scheme
#    env.hosts = nodes.keys()

    execute(set_active_backup, suffix=restore_domain)
    # First download the db this will take awhile, so can be skipped when not needed
    if prompt('Skip Download? [y/n] ') != 'y':
        day = prompt('Please specify YYYY_MM_DD of the backup you would like to restore:')
        execute(download_backup, restore_domain, dbname, day=day)

    # Switch to the new
    prompt(magenta('Ready to disable the running db and switch to the restored db, press enter to continue.'))
    execute(stop_db, hosts=primary_node)
    execute(switch_active_dataset, to_set='%s_%s' % (dbname, restore_domain),
            from_set='%s_%s' % (dbname, current_domain), dbname=dbname)
    try:
        execute(prep_restore, restore_domain, dbname)
        execute(vbr_restore, dbname, hosts=primary_node)
        #Link the server ssl certs again
        execute(ssl_link, dbname)
        execute(start_db, dbname, hosts=primary_node)
    except SystemExit:
        prompt(red('Restore error encountered press enter to revert to previous db setup.'))
    else:
        prompt(magenta('Verify the db restore worked then press enter to continue.'))
        execute(stop_db, hosts=primary_node)
    finally:
        #Revert back to the previous db version
        execute(unset_active_backup, suffix=restore_domain)
        # Save the existing database, the backup dir remains so a full restore is done each time
        execute(switch_active_dataset, to_set='%s_%s' % (dbname, current_domain),
                from_set='%s_%s' % (dbname, restore_domain), dbname=dbname)
        execute(start_db, dbname, hosts=primary_node)


@task
@parallel
def download_backup(domain, dbname, day=''):
    """ Download a Vertica backup from swift.
    """
    with settings(hide('running', 'output')):
        # set the snapshot name in dbname_backup.yaml
        sudo('cp /opt/vertica/config/%s_backup.yaml /opt/vertica/config/%s_backup.yaml-backup' % (dbname, dbname))
        sudo(
            "sed 's/^snapshot_name:.*/snapshot_name: %s/' /opt/vertica/config/%s_backup.yaml-backup > /opt/vertica/config/%s_backup.yaml" %
            (domain.replace('.', '_') + '_' + dbname, dbname, dbname)
        )

        # todo this assumes you are downloading to a cluster with an existing db
        data_v_node = sudo('ls /var/vertica/data/%s' % dbname)
        v_node = data_v_node[:data_v_node.index('_data')]

    sudo('vertica_restore_download /opt/vertica/config/%s_backup.yaml %s %s %s' % (dbname, domain, v_node, day))


@task
@runs_once
def get_cluster_nodes(dbname):
    """ For a vertica node in the run list discover the remaining nodes in the cluster returning a
    """
    nodes = {}
    with settings(hide('running', 'output')):
        for line in sudo('grep ^v_%s_node /opt/vertica/config/admintools.conf' % dbname).splitlines():
            name, ip = line.split(',')[0].split('=')
            nodes[name.strip()] = ip.strip()

    return nodes


@task
def prep_restore(domain, dbname):
    """ Prepare the backup for restore, performing all the steps needed to restore to an existing cluster.
    """
    #The backups sometimes have some rsync artifacts in them, remove these
    with(settings(hide('everything'), warn_only=True)):
        sudo('rm -f /var/vertica/data/backup/v_%s_node*/*/.deldelay*' % dbname)

    # config changesonly needed for restoring to a cluster with different ips, which is the case for all test restores, they are no-op otherwise.
    # update vbr snapshot name
    snapshot_name = domain.replace('.', '_') + '_' + dbname
    with(settings(hide('commands'))):
        sudo('sed "s/snapshotName =.*/snapshotName = %s/" /opt/vertica/config/%s_backup.ini > /tmp/%s_backup.ini' % (snapshot_name, dbname, dbname))
        sudo('cp /tmp/%s_backup.ini /opt/vertica/config/%s_backup.ini' % (dbname, dbname))

    # Edit the expected ips in the backup config putting in the cluster ips, easier to do in python
    # TODO this is all pretty ugly code, come up with a better way to do this. There are lots if the python is run where the files exist
    # but since it isn't I have to be creative.
    nodes = get_cluster_nodes(dbname)
    with settings(hide('running', 'output')):
        new_backup_info = tempfile.NamedTemporaryFile(delete=False)
        for line in sudo('cat /var/vertica/data/backup/v_%s_node*/*/*.info' % dbname).splitlines():
            if line.startswith('name:'):
                splits = line.split()
                new_backup_info.write(splits[0] + ' address:%s ' % nodes[splits[0].split(':')[1]] + splits[2] + "\n")
            else:
                new_backup_info.write(line + "\n")

        new_backup_info.close()
        with(settings(hide('everything'), warn_only=True)):
            sudo('rm -f /tmp/new_backup.info')
        put(new_backup_info.name, '/tmp/new_backup.info')
        sudo('cp /tmp/new_backup.info /var/vertica/data/backup/v_%s_node*/*/*.info' % dbname)
        os.remove(new_backup_info.name)

    #todo script this, if the file does not exist it is vertica 6 and can be skipped.
    prompt("If running Vertica 7 and doing a test restore to another cluster an additional file needs to be edited.\n" +
           "Change all backup ips to their restore equivalent in this file on each restore node, press enter when finished " +
           "/var/vertica/data/backup/v_*_node*/*/var/vertica/catalog/%s/v_*_node*_catalog/Snapshots" % dbname)


@task
def ssl_link(dbname=None):
    """ Link the ssl certs for Vertica into the catalog dir. """
    # Todo I should work on a class variable for dbname so not every single task needs to ask for it
    if dbname is None:
        dbname = prompt('Which db should be restored?')
    with settings(hide('everything'), warn_only=True):
        v7_location = sudo('ls /var/vertica/server*')
        if v7_location.succeeded:
            sudo('ln -s /var/vertica/server* /var/vertica/catalog/%s/v_%s_node000?_catalog/' % (dbname, dbname))
        else:  # Vertica 6 installs have the certs in a different configuration
            sudo('ln -s /var/vertica/catalog/server* /var/vertica/catalog/%s/v_%s_node000?_catalog/' % (dbname, dbname))


@task
@runs_once
def start_db(dbname=None):
    """ Start up vertica, run this on one box only"""
    if dbname is None:
        dbname = prompt('Which db should be restored?')
    dbpass = prompt('Please enter the db password needed to start up the database: ')
    with settings(hide('running')):
        sudo('/opt/vertica/bin/admintools -t start_db -d %s -p %s' % (dbname, dbpass), user='dbadmin')


@task
@runs_once
def stop_db():
    """ Stop vertica, run this on one box only"""
    puts(magenta('Stopping database'))
    with settings(warn_only=True):
        shutdown = sudo('/opt/vertica/bin/vsql -c "SELECT SHUTDOWN(true);"', user='dbadmin')  # Will prompt for the dbadmin password
    if shutdown.failed:
        if prompt('Warning the shutdown failed, possibly because no db was running, continue? [y/n] ') == 'n':
            abort('Aborting restore, db did not shutdown correctly.')


@parallel
def set_active_backup(suffix):
    """ Switch the active backup dir to allow restoring from multiple datasources to the same cluster.
    """
    #Chef runs will make the backup dir so I need to make sure it isn't there and empty
    with settings(hide('everything'), warn_only=True):
        backup_dir_exists = sudo('ls -d /var/vertica/data/backup').succeeded
        new_backup_dir_exists = sudo('ls -d /var/vertica/data/backup_%s' % suffix).succeeded
        if backup_dir_exists:
            sudo('rmdir /var/vertica/data/backup')  # Fails if it isn't empty
        if new_backup_dir_exists:
            sudo('mv /var/vertica/data/backup_%s /var/vertica/data/backup' % suffix)
        else:
            sudo('mkdir /var/vertica/data/backup')


@parallel
def unset_active_backup(suffix):
    """ Disable active backup dir.
    """
    #TODO make sure destination doesn't exist
    sudo('mv /var/vertica/data/backup /var/vertica/data/backup_%s' % suffix)


@parallel
def switch_active_dataset(to_set, from_set, dbname, delete_from=False):
    """ Switches the active data/catalog directories used by vertica.
        This is used during test restores to move aside dev data to test the restored data and then again
        to switch it back.
        The to_set is the name of the data/catalog to put in place.
        the from_set is the name the currently active set will be given.
        If delete_from is set instead of moving the files aside they are deleted.
    """
    #TODO: check to make sure the db is not running first

    data_basepath = '/var/vertica/data/'
    catalog_basepath = '/var/vertica/data/catalog_'
    link_basepath = '/var/vertica/catalog/'  # just the symbolic link

    with(settings(hide('everything'), warn_only=True)):
        sudo('rm -r {link_basepath}{dbname}'.format(link_basepath=link_basepath, dbname=dbname))
        if delete_from:
            sudo('rm -r {data_basepath}{dbname}'.format(data_basepath=data_basepath, dbname=dbname))
            sudo('rm -r {catalog_basepath}{dbname}'.format(catalog_basepath=catalog_basepath, dbname=dbname))
        else:
            sudo('mv {data_basepath}{dbname} {data_basepath}{from_set}'.format(data_basepath=data_basepath,
                                                                               dbname=dbname, from_set=from_set))
            sudo('mv {catalog_basepath}{dbname} {catalog_basepath}{from_set}'.format(catalog_basepath=catalog_basepath,
                                                                                     dbname=dbname, from_set=from_set))

    # If the to_set exists move it otherwise create empty dirs
    with(settings(hide('everything'), warn_only=True)):
        to_ls = sudo('ls {data_basepath}{to_set}'.format(data_basepath=data_basepath, to_set=to_set))
    if to_ls.succeeded:
        sudo('mv {data_basepath}{to_set} {data_basepath}{dbname}'.format(data_basepath=data_basepath,
                                                                         to_set=to_set, dbname=dbname))
        sudo('mv {catalog_basepath}{to_set} {catalog_basepath}{dbname}'.format(catalog_basepath=catalog_basepath,
                                                                               to_set=to_set, dbname=dbname))
    else:
        sudo('mkdir {data_basepath}{dbname}'.format(data_basepath=data_basepath, dbname=dbname), user='dbadmin')
        sudo('mkdir {catalog_basepath}{dbname}'.format(catalog_basepath=catalog_basepath, dbname=dbname),
             user='dbadmin')

    # vbr encounters 'Invalid cross-device link' when the catalog is on a different partition, despite this being the best practice setup
    sudo('ln -s {catalog_basepath}{dbname} {link_basepath}{dbname}'.format(catalog_basepath=catalog_basepath,
                                                                           link_basepath=link_basepath, dbname=dbname),
         user='dbadmin')


@task
@runs_once
def vbr_restore(dbname):
    """ Run the vbr restore command. This should only run on one node per cluster. """
    #with(settings(hide('everything'), warn_only = True)):
    with(settings(warn_only=True)):
        vbr = sudo('/opt/vertica/bin/vbr.py --task restore --config-file /opt/vertica/config/%s_backup.ini' % dbname, user='dbadmin')

    if vbr.failed:
        abort('The vbr restore command failed! Review logs in /tmp/vbr')
