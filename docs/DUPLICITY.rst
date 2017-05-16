Duplicity
===================================================

How to backup your files with S3 Server.

Installing
----------

Deploying S3
~~~~~~~~~~~~

First, you need to deploy **S3 Server**. This can be done very easily
via `our **DockerHub**
page <https://hub.docker.com/r/scality/s3server/>`__ (you want to run it
with a file backend).

    *Note:* *- If you don't have docker installed on your machine, here
    are the `instructions to install it for your
    distribution <https://docs.docker.com/engine/installation/>`__*

Installing Duplicity and its dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Second, you want to install
`**Duplicity** <http://duplicity.nongnu.org/index.html>`__. You have to
download `this
tarball <https://code.launchpad.net/duplicity/0.7-series/0.7.11/+download/duplicity-0.7.11.tar.gz>`__,
decompress it, and then checkout the README inside, which will give you
a list of dependencies to install. If you're using Ubuntu 14.04, this is
your lucky day: here is a lazy step by step install.

.. code:: sh

    $> apt-get install librsync-dev gnupg
    $> apt-get install python-dev python-pip python-lockfile
    $> pip install -U boto

Then you want to actually install Duplicity:

.. code:: sh

    $> tar zxvf duplicity-0.7.11.tar.gz
    $> cd duplicity-0.7.11
    $> python setup.py install

Using
-----

Testing your installation
~~~~~~~~~~~~~~~~~~~~~~~~~

First, we're just going to quickly check that S3 Server is actually
running. To do so, simply run ``$> docker ps`` . You should see one
container named ``scality/s3server``. If that is not the case, try
``$> docker start s3server``, and check again.

Secondly, as you probably know, Duplicity uses a module called **Boto**
to send requests to S3. Boto requires a configuration file located in
**``/etc/boto.cfg``** to have your credentials and preferences. Here is
a minimalistic config `that you can finetune following these
instructions <http://boto.cloudhackers.com/en/latest/getting_started.html>`__.

::

    [Credentials]
    aws_access_key_id = accessKey1
    aws_secret_access_key = verySecretKey1

    [Boto]
    # If using SSL, set to True
    is_secure = False
    # If using SSL, unmute and provide absolute path to local CA certificate
    # ca_certificates_file = /absolute/path/to/ca.crt

    *Note:* *If you want to set up SSL with S3 Server, check out our
    `tutorial <http://link/to/SSL/tutorial>`__*

At this point, we've met all the requirements to start running S3 Server
as a backend to Duplicity. So we should be able to back up a local
folder/file to local S3. Let's try with the duplicity decompressed
folder:

.. code:: sh

    $> duplicity duplicity-0.7.11 "s3://127.0.0.1:8000/testbucket/"

    *Note:* *Duplicity will prompt you for a symmetric encryption
    passphrase. Save it somewhere as you will need it to recover your
    data. Alternatively, you can also add the ``--no-encryption`` flag
    and the data will be stored plain.*

If this command is succesful, you will get an output looking like this:

::

    --------------[ Backup Statistics ]--------------
    StartTime 1486486547.13 (Tue Feb  7 16:55:47 2017)
    EndTime 1486486547.40 (Tue Feb  7 16:55:47 2017)
    ElapsedTime 0.27 (0.27 seconds)
    SourceFiles 388
    SourceFileSize 6634529 (6.33 MB)
    NewFiles 388
    NewFileSize 6634529 (6.33 MB)
    DeletedFiles 0
    ChangedFiles 0
    ChangedFileSize 0 (0 bytes)
    ChangedDeltaSize 0 (0 bytes)
    DeltaEntries 388
    RawDeltaSize 6392865 (6.10 MB)
    TotalDestinationSizeChange 2003677 (1.91 MB)
    Errors 0
    -------------------------------------------------

Congratulations! You can now backup to your local S3 through duplicity
:)

Automating backups
~~~~~~~~~~~~~~~~~~

Now you probably want to back up your files periodically. The easiest
way to do this is to write a bash script and add it to your crontab.
Here is my suggestion for such a file:

.. code:: sh

    #!/bin/bash

    # Export your passphrase so you don't have to type anything
    export PASSPHRASE="mypassphrase"

    # If you want to use a GPG Key, put it here and unmute the line below
    #GPG_KEY=

    # Define your backup bucket, with localhost specified
    DEST="s3://127.0.0.1:8000/testbuckets3server/"

    # Define the absolute path to the folder you want to backup
    SOURCE=/root/testfolder

    # Set to "full" for full backups, and "incremental" for incremental backups
    # Warning: you have to perform one full backup befor you can perform
    # incremental ones on top of it
    FULL=incremental

    # How long to keep backups for; if you don't want to delete old backups, keep
    # empty; otherwise, syntax is "1Y" for one year, "1M" for one month, "1D" for
    # one day
    OLDER_THAN="1Y"

    # is_running checks whether duplicity is currently completing a task
    is_running=$(ps -ef | grep duplicity  | grep python | wc -l)

    # If duplicity is already completing a task, this will simply not run
    if [ $is_running -eq 0 ]; then
        echo "Backup for ${SOURCE} started"

        # If you want to delete backups older than a certain time, we do it here
        if [ "$OLDER_THAN" != "" ]; then
            echo "Removing backups older than ${OLDER_THAN}"
            duplicity remove-older-than ${OLDER_THAN} ${DEST}
        fi

        # This is where the actual backup takes place
        echo "Backing up ${SOURCE}..."
        duplicity ${FULL} \
            ${SOURCE} ${DEST}
            # If you're using GPG, paste this in the command above
            # --encrypt-key=${GPG_KEY} --sign-key=${GPG_KEY} \
            # If you want to exclude a subfolder/file, put it below and paste this
            # in the command above
            # --exclude=/${SOURCE}/path_to_exclude \

        echo "Backup for ${SOURCE} complete"
        echo "------------------------------------"
    fi
    # Forget the passphrase...
    unset PASSPHRASE

So let's say you put this file in ``/usr/local/sbin/backup.sh.`` Next
you want to run ``crontab -e`` and paste your configuration in the file
that opens. If you're unfamiliar with Cron, here is a good `How
To <https://help.ubuntu.com/community/CronHowto>`__. The folder I'm
backing up is a folder I modify permanently during my workday, so I want
incremental backups every 5mn from 8AM to 9PM monday to friday. Here is
the line I will paste in my crontab:

.. code:: cron

    */5 8-20 * * 1-5 /usr/local/sbin/backup.sh

Now I can try and add / remove files from the folder I'm backing up, and
I will see incremental backups in my bucket.
