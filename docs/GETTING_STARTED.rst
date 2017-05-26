Getting Started
=================

.. figure:: ../res/Scality-S3-Server-Logo-Large.png
   :alt: S3 Server logo

|CircleCI| |Scality CI|

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

.. |CircleCI| image:: https://circleci.com/gh/scality/S3.svg?style=svg
   :target: https://circleci.com/gh/scality/S3
.. |Scality CI| image:: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae
   :target: http://ci.ironmann.io/gh/scality/S3
