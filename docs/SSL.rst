Scality with SSL
================

If you wish to use https with your local S3 Server, you need to set up
SSL certificates. Here is a simple guide of how to do it.

Deploying S3 Server
-------------------

First, you need to deploy **S3 Server**. This can be done very easily
via `our **DockerHub**
page <https://hub.docker.com/r/scality/s3server/>`__ (you want to run it
with a file backend).

    *Note:* *- If you don't have docker installed on your machine, here
    are the `instructions to install it for your
    distribution <https://docs.docker.com/engine/installation/>`__*

Updating your S3 Server container's config
------------------------------------------

You're going to add your certificates to your container. In order to do
so, you need to exec inside your s3 server container. Run a
``$> docker ps`` and find your container's id (the corresponding image
name should be ``scality/s3server``. Copy the corresponding container id
(here we'll use ``894aee038c5e``, and run:

.. code:: sh

    $> docker exec -it 894aee038c5e bash

You're now inside your container, using an interactive terminal :)

Generate SSL key and certificates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are 5 steps to this generation. The paths where the different
files are stored are defined after the ``-out`` option in each command

.. code:: sh

    # Generate a private key for your CSR
    $> openssl genrsa -out ca.key 2048
    # Generate a self signed certificate for your local Certificate Authority
    $> openssl req -new -x509 -extensions v3_ca -key ca.key -out ca.crt -days 99999  -subj "/C=US/ST=Country/L=City/O=Organization/CN=scality.test"

    # Generate a key for S3 Server
    $> openssl genrsa -out test.key 2048
    # Generate a Certificate Signing Request for S3 Server
    $> openssl req -new -key test.key -out test.csr -subj "/C=US/ST=Country/L=City/O=Organization/CN=*.scality.test"
    # Generate a local-CA-signed certificate for S3 Server
    $> openssl x509 -req -in test.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out test.crt -days 99999 -sha256

Update S3Server ``config.json``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a ``certFilePaths`` section to ``./config.json`` with the
appropriate paths:

.. code:: json

        "certFilePaths": {
            "key": "./test.key",
            "cert": "./test.crt",
            "ca": "./ca.crt"
        }

Run your container with the new config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to exit your container. Simply run ``$> exit``. Then,
you need to restart your container. Normally, a simple
``$> docker restart s3server`` should do the trick.

Update your host config
-----------------------

Associates local IP addresses with hostname
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In your ``/etc/hosts`` file on Linux, OS X, or Unix (with root
permissions), edit the line of localhost so it looks like this:

::

    127.0.0.1      localhost s3.scality.test

Copy the local certificate authority from your container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the above commands, it's the file named ``ca.crt``. Choose the path
you want to save this file at (here we chose ``/root/ca.crt``), and run
something like:

.. code:: sh

    $> docker cp 894aee038c5e:/usr/src/app/ca.crt /root/ca.crt

Test your config
----------------

If you do not have aws-sdk installed, run ``$> npm install aws-sdk``. In
a ``test.js`` file, paste the following script:

.. code:: js

    const AWS = require('aws-sdk');
    const fs = require('fs');
    const https = require('https');

    const httpOptions = {
        agent: new https.Agent({
            // path on your host of the self-signed certificate
            ca: fs.readFileSync('./ca.crt', 'ascii'),
        }),
    };

    const s3 = new AWS.S3({
        httpOptions,
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
        // The endpoint must be s3.scality.test, else SSL will not work
        endpoint: 'https://s3.scality.test:8000',
        sslEnabled: true,
        // With this setup, you must use path-style bucket access
        s3ForcePathStyle: true,
    });

    const bucket = 'cocoriko';

    s3.createBucket({ Bucket: bucket }, err => {
        if (err) {
            return console.log('err createBucket', err);
        }
        return s3.deleteBucket({ Bucket: bucket }, err => {
            if (err) {
                return console.log('err deleteBucket', err);
            }
            return console.log('SSL is cool!');
        });
    });

Now run that script with ``$> nodejs test.js``. If all goes well, it
should output ``SSL is cool!``. Enjoy that added security!
