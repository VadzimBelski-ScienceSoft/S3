S3FS
====
Export your buckets as a filesystem with s3fs on top of s3server

`**s3fs** <https://github.com/s3fs-fuse/s3fs-fuse>`__ is an open source
tool that allows you to mount an S3 bucket on a filesystem-like backend.
It is available both on Debian and RedHat distributions. For this
tutorial, we used an Ubuntu 14.04 host to deploy and use s3fs over
Scality's S3 Server.

Deploying S3 Server with SSL
----------------------------

First, you need to deploy **S3 Server**. This can be done very easily
via `our **DockerHub**
page <https://hub.docker.com/r/scality/s3server/>`__ (you want to run it
with a file backend).

    *Note:* *- If you don't have docker installed on your machine, here
    are the `instructions to install it for your
    distribution <https://docs.docker.com/engine/installation/>`__*

You also necessarily have to set up SSL with S3Server to use s3fs. We
have a nice
`tutorial <https://s3.scality.com/v1.0/page/scality-with-ssl>`__ to help
you do it.

s3fs setup
----------

Installing s3fs
~~~~~~~~~~~~~~~

s3fs has quite a few dependencies. As explained in their
`README <https://github.com/s3fs-fuse/s3fs-fuse/blob/master/README.md#installation>`__,
the following commands should install everything for Ubuntu 14.04:

.. code:: sh

    $> sudo apt-get install automake autotools-dev g++ git libcurl4-gnutls-dev
    $> sudo apt-get install libfuse-dev libssl-dev libxml2-dev make pkg-config

Now you want to install s3fs per se:

.. code:: sh

    $> git clone https://github.com/s3fs-fuse/s3fs-fuse.git
    $> cd s3fs-fuse
    $> ./autogen.sh
    $> ./configure
    $> make
    $> sudo make install

Check that s3fs is properly installed by checking its version. it should
answer as below:

.. code:: sh

     $> s3fs --version

    Amazon Simple Storage Service File System V1.80(commit:d40da2c) with OpenSSL

Configuring s3fs
~~~~~~~~~~~~~~~~

s3fs expects you to provide it with a password file. Our file is
``/etc/passwd-s3fs``. The structure for this file is
``ACCESSKEYID:SECRETKEYID``, so, for S3Server, you can run:

.. code:: sh

    $> echo 'accessKey1:verySecretKey1' > /etc/passwd-s3fs
    $> chmod 600 /etc/passwd-s3fs

Using S3Server with s3fs
------------------------

First, you're going to need a mountpoint; we chose ``/mnt/tests3fs``:

.. code:: sh

    $> mkdir /mnt/tests3fs

Then, you want to create a bucket on your local S3Server; we named it
``tests3fs``:

.. code:: sh

    $> s3cmd mb s3://tests3fs

    *Note:* *- If you've never used s3cmd with our S3Server, our README
    provides you with a `recommended
    config <https://github.com/scality/S3/blob/master/README.md#s3cmd>`__*

Now you can mount your bucket to your mountpoint with s3fs:

.. code:: sh

    $> s3fs tests3fs /mnt/tests3fs -o passwd_file=/etc/passwd-s3fs -o url="https://s3.scality.test:8000/" -o use_path_request_style

    *If you're curious, the structure of this command is*
    ``s3fs BUCKET_NAME PATH/TO/MOUNTPOINT -o OPTIONS``\ *, and the
    options are mandatory and serve the following purposes:
    * ``passwd_file``\ *: specifiy path to password file;
    * ``url``\ *: specify the hostname used by your SSL provider;
    * ``use_path_request_style``\ *: force path style (by default, s3fs
    uses subdomains (DNS style)).*

| From now on, you can either add files to your mountpoint, or add
  objects to your bucket, and they'll show in the other.
| For example, let's' create two files, and then a directory with a file
  in our mountpoint:

.. code:: sh

    $> touch /mnt/tests3fs/file1 /mnt/tests3fs/file2
    $> mkdir /mnt/tests3fs/dir1
    $> touch /mnt/tests3fs/dir1/file3

Now, I can use s3cmd to show me what is actually in S3Server:

.. code:: sh

    $> s3cmd ls -r s3://tests3fs

    2017-02-28 17:28         0   s3://tests3fs/dir1/
    2017-02-28 17:29         0   s3://tests3fs/dir1/file3
    2017-02-28 17:28         0   s3://tests3fs/file1
    2017-02-28 17:28         0   s3://tests3fs/file2

Now you can enjoy a filesystem view on your local S3Server!
