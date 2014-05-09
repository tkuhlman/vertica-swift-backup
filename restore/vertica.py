""" Retore Vertica from Swift backups

    I leverage fabric for this and use the existing commands to discover hosts. It is assumed these boxes are setup in chef using the SOM team recipes.
    Most of the restore task will be the entry point

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

# todo remove the som refrences and make that db name configurable
# todo setup detection of backup_dir, data_dir and catalog_dir from the the config files and remove references to /var/vertica, update readme


@task
@runs_once
def restore():
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
    restore_domain = prompt('Which domain should be restored?')

    # Discover details of the cluster
    primary_node = env.hosts[0]
    cluster_nodes = get_cluster_nodes()
    nodes = {}  # 'fqdn':'v_node name'
    for vnode in cluster_nodes.iterkeys():
        cluster_fqdn = socket.gethostbyaddr(cluster_nodes[vnode])[0]
        nodes[cluster_fqdn.replace('-cluster', '')] = vnode  # This relies on the cluster ip naming scheme used by som
    env.hosts = nodes.keys()

    execute(set_active_backup, suffix=restore_domain)
    # First download the db this will take awhile, so can be skipped when not needed
    if prompt('Skip Download? [y/n] ') != 'y':
        day = prompt('Please specify YYYY_MM_DD of the backup you would like to restore:')
        execute(download_backup, restore_domain, day=day)

    # Switch to the new
    prompt(magenta('Ready to disable the running db and switch to the restored db, press enter to continue.'))
    execute(stop_db, hosts=primary_node)
    execute(switch_active_dataset, to_set='som_%s' % restore_domain, from_set='som_%s' % current_domain)
    try:
        execute(prep_restore, restore_domain)
        execute(vbr_restore, hosts=primary_node)
        #Link the server ssl certs again
        execute(ssl_link)
        execute(start_db, hosts=primary_node)
    except SystemExit:
        prompt(red('Restore error encountered press enter to revert to previous db setup.'))
    else:
        prompt(magenta('Verify the db restore worked then press enter to continue.'))
        execute(stop_db, hosts=primary_node)
    finally:
        #Revert back to the previous db version
        execute(unset_active_backup, suffix=restore_domain)
        # Delete the restored data/catalog, the backup dir remains so a full restore is done each time
        execute(switch_active_dataset, to_set='som_%s' % current_domain, from_set='som_%s' % restore_domain, delete_from=True)
        execute(start_db, hosts=primary_node)


@task
@parallel
def download_backup(domain, dbname='som', day=''):
    """ Download a Vertica backup from swift.
    """
    with settings(hide('running', 'output')):
        # set the snapshot name in som_backup.yaml
        sudo('cp /opt/vertica/config/%s_backup.yaml /opt/vertica/config/%s_backup.yaml-backup' % (dbname, dbname))
        sudo(
            "sed 's/^snapshot_name:.*/snapshot_name: %s/' /opt/vertica/config/%s_backup.yaml-backup > /opt/vertica/config/%s_backup.yaml" %
            (domain.replace('.', '_') + '_' + dbname, dbname, dbname)
        )

        data_v_node = sudo('ls /var/vertica/data/%s' % dbname)
        v_node = data_v_node[:data_v_node.index('_data')]

    sudo('vertica_restore_download /opt/vertica/config/som_backup.yaml %s %s %s' % (domain, v_node, day))


@task
@runs_once
def get_cluster_nodes(dbname='som'):
    """ For a vertica node in the run list discover the remaining nodes in the cluster returning a
    """
    nodes = {}
    with settings(hide('running', 'output')):
        for line in sudo('grep ^v_%s_node /opt/vertica/config/admintools.conf' % dbname).splitlines():
            name, ip = line.split(',')[0].split('=')
            nodes[name.strip()] = ip.strip()

    return nodes


@parallel
def prep_restore(domain, dbname='som'):
    """ Prepare the backup for restore, performing all the steps needed to restore to an existing cluster.
    """
    #The backups sometimes have some rsync artifacts in them, remove these
    with(settings(hide('everything'), warn_only=True)):
        sudo('rm /var/vertica/data/backup/v_som_node*/*/.deldelay*')

    # config changesonly needed for restoring to a cluster with different ips, which is the case for all test restores, they are no-op otherwise.
    # update vbr snapshot name
    snapshot_name = domain.replace('.', '_') + '_' + dbname
    with(settings(hide('commands'))):
        sudo('sed "s/snapshotName =.*/snapshotName = %s/" /opt/vertica/config/som_backup.ini > /tmp/som_backup.ini' % snapshot_name)
        sudo('cp /tmp/som_backup.ini /opt/vertica/config/som_backup.ini')

    # Edit the expected ips in the backup config putting in the cluster ips, easier to do in python
    # TODO this is all pretty ugly code, come up with a better way to do this. There are lots if the python is run where the files exist
    # but since it isn't I have to be creative.
    nodes = get_cluster_nodes()
    with settings(hide('running', 'output')):
        new_backup_info = tempfile.NamedTemporaryFile(delete=False)
        tmp_file = new_backup_info.name
        for line in sudo('cat /var/vertica/data/backup/v_som_node*/*/*.info').splitlines():
            if line.startswith('name:'):
                splits = line.split()
                new_backup_info.write(splits[0] + ' address:%s ' % nodes[splits[0].split(':')[1]] + splits[2] + "\n")
            else:
                new_backup_info.write(line + "\n")

        new_backup_info.close()
        #For some reason basenode does not have sftp turned on
        #put(new_backup_info.name, '/tmp/new_backup.info')
        local('scp %s %s:/tmp/new_backup.info' % (tmp_file, env.host_string))
        sudo('cp /tmp/new_backup.info /var/vertica/data/backup/v_som_node*/*/*.info')
        os.remove(tmp_file)


@task
@parallel
def ssl_link():
    """ Link the ssl certs for Vertica into the catalog dir. """
    sudo('ln -s /var/vertica/catalog/server* /var/vertica/catalog/som/v_som_node000?_catalog/')


@task
@runs_once
def start_db():
    """ Start up vertica, run this on one box only"""
    dbpass = prompt('Please enter the db password needed to start up the database: ')
    with settings(hide('running')):
        sudo('/opt/vertica/bin/admintools -t start_db -d som -p %s' % dbpass, user='dbadmin')


@task
@runs_once
def stop_db():
    """ Stop vertica, run this on one box only"""
    puts(magenta('Stopping database'))
    sudo('/opt/vertica/bin/vsql -c "SELECT SHUTDOWN(true);"', user='dbadmin')  # Will prompt for the dbadmin password


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
def switch_active_dataset(to_set, from_set, dbname='som', delete_from=False):
    """ Switches the active data/catalog directories used by vertica.
        This is used during test restores to move aside dev data to test the restored data and then again
        to switch it back.
        The to_set is the name of the data/catalog to put in place.
        the from_set is the name the currently active set will be given.
        If delete_from is set instead of moving the files aside they are deleted.
    """
    #TODO: check to make sure the db is not running first

    if delete_from:
        sudo('rm -r /var/vertica/data/%s' % dbname)
        sudo('rm -r /var/vertica/catalog/%s' % dbname)  # just the symbolic link
        sudo('rm -r /var/vertica/data/catalog_%s' % dbname)
    else:
        sudo('mv /var/vertica/data/%s /var/vertica/data/%s' % (dbname, from_set))
        sudo('mv /var/vertica/catalog/%s /var/vertica/catalog/%s' % (dbname, from_set))

    # If the to_set exists move it otherwise create empty dirs
    with(settings(hide('everything'), warn_only=True)):
        to_ls = sudo('ls /var/vertica/data/%s' % to_set)
    if to_ls.succeeded:
        sudo('mv /var/vertica/data/%s /var/vertica/data/%s' % (to_set, dbname))
        sudo('mv /var/vertica/catalog/%s /var/vertica/catalog/%s' % (to_set, dbname))
    else:
        sudo('mkdir /var/vertica/data/%s' % dbname, user='dbadmin')
        # vbr encounters 'Invalid cross-device link' when the catalog is on a different partition, despite this being the best practice setup
        sudo('mkdir /var/vertica/data/catalog_%s' % dbname, user='dbadmin')
        sudo('ln -s /var/vertica/data/catalog_%s /var/vertica/catalog/%s' % (dbname, dbname), user='dbadmin')


@task
@runs_once
def vbr_restore():
    """ Run the vbr restore command. This should only run on one node per cluster. """
    #with(settings(hide('everything'), warn_only = True)):
    with(settings(warn_only=True)):
        vbr = sudo('/opt/vertica/bin/vbr.py --task restore --config-file /opt/vertica/config/som_backup.ini', user='dbadmin')

    if vbr.failed:
        abort('The vbr restore command failed! Review logs in /tmp/vbr')
