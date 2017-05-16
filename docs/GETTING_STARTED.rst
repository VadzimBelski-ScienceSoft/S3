Getting Started
=================

.. figure:: ../res/Scality-S3-Server-Logo-Large.png
   :alt: S3 Server logo

|CircleCI| |Scality CI|

Learn more at `s3.scality.com <http://s3.scality.com>`__
--------------------------------------------------------

Contributing
------------

In order to contribute, please follow the `Contributing
Guidelines <https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md>`__.

Installation
------------

Dependencies
~~~~~~~~~~~~

Building and running the Scality S3 Server requires node.js 6.9.5 and
npm v3 . Up-to-date versions can be found at
`Nodesource <https://github.com/nodesource/distributions>`__.

Clone source code
~~~~~~~~~~~~~~~~~

.. code:: shell

    git clone https://github.com/scality/S3.git

Install js dependencies
~~~~~~~~~~~~~~~~~~~~~~~

Go to the ./S3 folder,

.. code:: shell

    npm install

Run it with a file backend
--------------------------

.. code:: shell

    npm start

This starts an S3 server on port 8000. Two additional ports 9990 and
9991 are also open locally for internal transfer of metadata and data,
respectively.

The default access key is accessKey1 with a secret key of
verySecretKey1.

By default the metadata files will be saved in the localMetadata
directory and the data files will be saved in the localData directory
within the ./S3 directory on your machine. These directories have been
pre-created within the repository. If you would like to save the data or
metadata in different locations of your choice, you must specify them
with absolute paths. So, when starting the server:

.. code:: shell

    mkdir -m 700 $(pwd)/myFavoriteDataPath
    mkdir -m 700 $(pwd)/myFavoriteMetadataPath
    export S3DATAPATH="$(pwd)/myFavoriteDataPath"
    export S3METADATAPATH="$(pwd)/myFavoriteMetadataPath"
    npm start

Run it with multiple data backends
----------------------------------

.. code:: shell

    export S3DATA='multiple'
    npm start

This starts an S3 server on port 8000. The default access key is
accessKey1 with a secret key of verySecretKey1.

With multiple backends, you have the ability to choose where each object
will be saved by setting the following header with a locationConstraint
on a PUT request:

.. code:: shell

    'x-amz-meta-scal-location-constraint':'myLocationConstraint'

If no header is sent with a PUT object request, the location constraint
of the bucket will determine where the data is saved. If the bucket has
no location constraint, the endpoint of the PUT request will be used to
determine location.

See the Configuration section below to learn how to set location
constraints.

Run it with an in-memory backend
--------------------------------

.. code:: shell

    npm run mem_backend

This starts an S3 server on port 8000. The default access key is
accessKey1 with a secret key of verySecretKey1.

Setting your own access key and secret key pairs
------------------------------------------------

You can set credentials for many accounts by editing
``conf/authdata.json`` but if you want to specify one set of your own
credentials, you can use ``SCALITY_ACCESS_KEY_ID`` and
``SCALITY_SECRET_ACCESS_KEY`` environment variables.

SCALITY\_ACCESS\_KEY\_ID and SCALITY\_SECRET\_ACCESS\_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These variables specify authentication credentials for an account named
"CustomAccount".

Note: Anything in the ``authdata.json`` file will be ignored.

.. code:: shell

    SCALITY_ACCESS_KEY_ID=newAccessKey SCALITY_SECRET_ACCESS_KEY=newSecretKey npm start

Run it for continuous integration testing or in production with Docker
----------------------------------------------------------------------

`DOCKER.md <DOCKER.md>`__

Testing
-------

You can run the unit tests with the following command:

.. code:: shell

    npm test

You can run the multiple backend unit tests with:

.. code:: shell

    npm run multiple_backend_test

You can run the linter with:

.. code:: shell

    npm run lint

Running functional tests locally:

The test suite requires additional tools, **s3cmd** and **Redis**
installed in the environment the tests are running in.

-  Install `s3cmd <http://s3tools.org/download>`__
-  Install `redis <https://redis.io/download>`__ and start Redis.
-  Add localCache section to your ``config.json``:

::

    "localCache": {
        "host": REDIS_HOST,
        "port": REDIS_PORT
    }

where ``REDIS_HOST`` is your Redis instance IP address (``"127.0.0.1"``
if your Redis is running locally) and ``REDIS_PORT`` is your Redis
instance port (``6379`` by default)

-  Add the following to the etc/hosts file on your machine:

.. code:: shell

    127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com

-  Start the S3 server in memory and run the functional tests:

.. code:: shell

    npm run mem_backend
    npm run ft_test

Configuration
-------------

There are three configuration files for your Scality S3 Server:

1. ``conf/authdata.json``, described above for authentication

2. ``locationConfig.json``, to set up configuration options for

   where data will be saved

3. ``config.json``, for general configuration options

Location Configuration
~~~~~~~~~~~~~~~~~~~~~~

You must specify at least one locationConstraint in your
locationConfig.json (or leave as pre-configured).

For instance, the following locationConstraint will save data sent to
``myLocationConstraint`` to the file backend:

.. code:: json

    "myLocationConstraint": {
        "type": "file",
        "legacyAwsBehavior": false,
        "details": {}
    },

Each locationConstraint must include the ``type``,
``legacyAwsBehavior``, and ``details`` keys. ``type`` indicates which
backend will be used for that region. Currently, mem, file, and scality
are the supported backends. ``legacyAwsBehavior`` indicates whether the
region will have the same behavior as the AWS S3 'us-east-1' region. If
the locationConstraint type is scality, ``details`` should contain
connector information for sproxyd. If the locationConstraint type is mem
or file, ``details`` should be empty.

Once you have your locationConstraints in your locationConfig.json, you
can specify a default locationConstraint for each of your endpoints.

For instance, the following sets the ``localhost`` endpoint to the
``myLocationConstraint`` data backend defined above:

.. code:: json

    "restEndpoints": {
         "localhost": "myLocationConstraint"
    },

If you would like to use an endpoint other than localhost for your
Scality S3 Server, that endpoint MUST be listed in your
``restEndpoints``. Otherwise if your server is running with a:

-  **file backend**: your default location constraint will be ``file``

-  **memory backend**: your default location constraint will be ``mem``

Endpoints
---------

Note that our S3server supports both:

-  path-style: http://myhostname.com/mybucket
-  hosted-style: http://mybucket.myhostname.com

However, hosted-style requests will not hit the server if you are using
an ip address for your host. So, make sure you are using path-style
requests in that case. For instance, if you are using the AWS SDK for
JavaScript, you would instantiate your client like this:

.. code:: js

    const s3 = new aws.S3({
       endpoint: 'http://127.0.0.1:8000',
       s3ForcePathStyle: true,
    });

Getting started: List of applications that have been tested with S3 Server
--------------------------------------------------------------------------

GUI
~~~

`Cyberduck <https://cyberduck.io/?l=en>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://www.youtube.com/watch?v=-n2MCt4ukUg
-  https://www.youtube.com/watch?v=IyXHcu4uqgU

`Cloud Explorer <https://www.linux-toys.com/?p=945>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://www.youtube.com/watch?v=2hhtBtmBSxE

`CloudBerry Lab <http://www.cloudberrylab.com>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://youtu.be/IjIx8g\_o0gY

Command Line Tools
~~~~~~~~~~~~~~~~~~

`s3curl <https://github.com/rtdp/s3curl>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

https://github.com/scality/S3/blob/master/tests/functional/s3curl/s3curl.pl

`aws-cli <http://docs.aws.amazon.com/cli/latest/reference/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``~/.aws/credentials`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.aws\credentials`` on Windows

.. code:: shell

    [default]
    aws_access_key_id = accessKey1
    aws_secret_access_key = verySecretKey1

``~/.aws/config`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.aws\config`` on Windows

.. code:: shell

    [default]
    region = us-east-1

Note: ``us-east-1`` is the default region, but you can specify any
region.

See all buckets:

.. code:: shell

    aws s3 ls --endpoint-url=http://localhost:8000

Create bucket:

.. code:: shell

    aws --endpoint-url=http://localhost:8000 s3 mb s3://mybucket

`s3cmd <http://s3tools.org/s3cmd>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If using s3cmd as a client to S3 be aware that v4 signature format is
buggy in s3cmd versions < 1.6.1.

``~/.s3cfg`` on Linux, OS X, or Unix or ``C:\Users\USERNAME\.s3cfg`` on
Windows

.. code:: shell

    [default]
    access_key = accessKey1
    secret_key = verySecretKey1
    host_base = localhost:8000
    host_bucket = %(bucket).localhost:8000
    signature_v2 = False
    use_https = False

See all buckets:

.. code:: shell

    s3cmd ls

`rclone <http://rclone.org/s3/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``~/.rclone.conf`` on Linux, OS X, or Unix or
``C:\Users\USERNAME\.rclone.conf`` on Windows

.. code:: shell

    [remote]
    type = s3
    env_auth = false
    access_key_id = accessKey1
    secret_access_key = verySecretKey1
    region = other-v2-signature
    endpoint = http://localhost:8000
    location_constraint =
    acl = private
    server_side_encryption =
    storage_class =

See all buckets:

.. code:: shell

    rclone lsd remote:

JavaScript
~~~~~~~~~~

`AWS JavaScript SDK <http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: javascript

    const AWS = require('aws-sdk');

    const s3 = new AWS.S3({
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
        endpoint: 'localhost:8000',
        sslEnabled: false,
        s3ForcePathStyle: true,
    });

JAVA
~~~~

`AWS JAVA SDK <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: java

    import com.amazonaws.auth.AWSCredentials;
    import com.amazonaws.auth.BasicAWSCredentials;
    import com.amazonaws.services.s3.AmazonS3;
    import com.amazonaws.services.s3.AmazonS3Client;
    import com.amazonaws.services.s3.S3ClientOptions;
    import com.amazonaws.services.s3.model.Bucket;

    public class S3 {

        public static void main(String[] args) {

            AWSCredentials credentials = new BasicAWSCredentials("accessKey1",
            "verySecretKey1");

            // Create a client connection based on credentials
            AmazonS3 s3client = new AmazonS3Client(credentials);
            s3client.setEndpoint("http://localhost:8000");
            // Using path-style requests
            // (deprecated) s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
            s3client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());

            // Create bucket
            String bucketName = "javabucket";
            s3client.createBucket(bucketName);

            // List off all buckets
            for (Bucket bucket : s3client.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
        }
    }

Ruby
----

`AWS SDK for Ruby - Version 2 <http://docs.aws.amazon.com/sdkforruby/api/>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ruby

    require 'aws-sdk'

    s3 = Aws::S3::Client.new(
      :access_key_id => 'accessKey1',
      :secret_access_key => 'verySecretKey1',
      :endpoint => 'http://localhost:8000',
      :force_path_style => true
    )

    resp = s3.list_buckets

`fog <http://fog.io/storage/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ruby

    require "fog"

    connection = Fog::Storage.new(
    {
        :provider => "AWS",
        :aws_access_key_id => 'accessKey1',
        :aws_secret_access_key => 'verySecretKey1',
        :endpoint => 'http://localhost:8000',
        :path_style => true,
        :scheme => 'http',
    })

Python
~~~~~~

`boto2 <http://boto.cloudhackers.com/en/latest/ref/s3.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import boto
    from boto.s3.connection import S3Connection, OrdinaryCallingFormat


    connection = S3Connection(
        aws_access_key_id='accessKey1',
        aws_secret_access_key='verySecretKey1',
        is_secure=False,
        port=8000,
        calling_format=OrdinaryCallingFormat(),
        host='localhost'
    )

    connection.create_bucket('mybucket')

`boto3 <http://boto3.readthedocs.io/en/latest/index.html>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import boto3
    client = boto3.client(
        's3',
        aws_access_key_id='accessKey1',
        aws_secret_access_key='verySecretKey1',
        endpoint_url='http://localhost:8000'
    )

    lists = client.list_buckets()

PHP
~~~

Should use v3 over v2 because v2 would create virtual-hosted style URLs
while v3 generates path-style URLs.

`AWS PHP SDK v3 <https://docs.aws.amazon.com/aws-sdk-php/v3/guide>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: php

    use Aws\S3\S3Client;

    $client = S3Client::factory([
        'region'  => 'us-east-1',
        'version'   => 'latest',
        'endpoint' => 'http://localhost:8000',
        'credentials' => [
             'key'    => 'accessKey1',
             'secret' => 'verySecretKey1'
        ]
    ]);

    $client->createBucket(array(
        'Bucket' => 'bucketphp',
    ));

.. |CircleCI| image:: https://circleci.com/gh/scality/S3.svg?style=svg
   :target: https://circleci.com/gh/scality/S3
.. |Scality CI| image:: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae
   :target: http://ci.ironmann.io/gh/scality/S3
