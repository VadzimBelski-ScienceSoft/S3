High Availability
=====================================

`Docker swarm <https://docs.docker.com/engine/swarm/>`__ is a
clustering tool developped by Docker and ready to use with its
containers. It allows to start a service, which we define and use as a
means to ensure s3server's continuous availability to the end user.
Indeed, a swarm defines a manager and n workers among n+1 servers. We
will do a basic setup in this tutorial, with just 3 servers, which
already provides a strong service resiliency, whilst remaining easy to
do as an individual. We will use NFS through docker to share data and
metadata between the different servers.

You will see that the steps of this tutorial are defined as **On
Server**, **On Clients**, **On All Machines**. This refers respectively
to NFS Server, NFS Clients, or NFS Server and Clients. In our example,
the IP of the Server will be **10.200.15.113**, while the IPs of the
Clients will be **10.200.15.96 and 10.200.15.97**

Installing docker
-----------------

Any version from docker 1.12.6 onwards should work; we used Docker
17.03.0-ce for this tutorial.

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04
^^^^^^^^^^^^^^^

The docker website has `solid
documentation <https://docs.docker.com/engine/installation/linux/ubuntu/>`__.
We have chosen to install the aufs dependency, as recommended by Docker.
Here are the required commands:

.. code:: sh

    $> sudo apt-get update
    $> sudo apt-get install linux-image-extra-$(uname -r) linux-image-extra-virtual
    $> sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
    $> curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    $> sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    $> sudo apt-get update
    $> sudo apt-get install docker-ce

On CentOS 7
^^^^^^^^^^^

The docker website has `solid
documentation <https://docs.docker.com/engine/installation/linux/centos/>`__.
Here are the required commands:

.. code:: sh

    $> sudo yum install -y yum-utils
    $> sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    $> sudo yum makecache fast
    $> sudo yum install docker-ce
    $> sudo systemctl start docker

Configure NFS
-------------

On Clients
~~~~~~~~~~

Your NFS Clients will mount Docker volumes over your NFS Server's shared
folders. Hence, you don't have to mount anything manually, you just have
to install the NFS commons:

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Simply install the NFS commons:

.. code:: sh

    $> sudo apt-get install nfs-common

On CentOS 7
^^^^^^^^^^^

Install the NFS utils, and then start the required services:

.. code:: sh

    $> yum install nfs-utils
    $> sudo systemctl enable rpcbind
    $> sudo systemctl enable nfs-server
    $> sudo systemctl enable nfs-lock
    $> sudo systemctl enable nfs-idmap
    $> sudo systemctl start rpcbind
    $> sudo systemctl start nfs-server
    $> sudo systemctl start nfs-lock
    $> sudo systemctl start nfs-idmap

On Server
~~~~~~~~~

Your NFS Server will be the machine to physically host the data and
metadata. The package(s) we will install on it is slightly different
from the one we installed on the clients.

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Install the NFS server specific package and the NFS commons:

.. code:: sh

    $> sudo apt-get install nfs-kernel-server nfs-common

On CentOS 7
^^^^^^^^^^^

Same steps as with the client: install the NFS utils and start the
required services:

.. code:: sh

    $> yum install nfs-utils
    $> sudo systemctl enable rpcbind
    $> sudo systemctl enable nfs-server
    $> sudo systemctl enable nfs-lock
    $> sudo systemctl enable nfs-idmap
    $> sudo systemctl start rpcbind
    $> sudo systemctl start nfs-server
    $> sudo systemctl start nfs-lock
    $> sudo systemctl start nfs-idmap

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Choose where your shared data and metadata from your local `S3
Server <http://www.scality.com/scality-s3-server/>`__ will be stored.
We chose to go with /var/nfs/data and /var/nfs/metadata. You also need
to set proper sharing permissions for these folders as they'll be shared
over NFS:

.. code:: sh

    $> mkdir -p /var/nfs/data /var/nfs/metadata
    $> chmod -R 777 /var/nfs/

Now you need to update your **/etc/exports** file. This is the file that
configures network permissions and rwx permissions for NFS access. By
default, Ubuntu applies the no\_subtree\_check option, so we declared
both folders with the same permissions, even though they're in the same
tree:

.. code:: sh

    $> sudo vim /etc/exports

In this file, add the following lines:

.. code:: sh

    /var/nfs/data        10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)
    /var/nfs/metadata    10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)

Export this new NFS table:

.. code:: sh

    $> sudo exportfs -a

Eventually, you need to allow for NFS mount from Docker volumes on other
machines. You need to change the Docker config in
**/lib/systemd/system/docker.service**:

.. code:: sh

    $> sudo vim /lib/systemd/system/docker.service

In this file, change the **MountFlags** option:

.. code:: sh

    MountFlags=shared

Now you just need to restart the NFS server and docker daemons so your
changes apply.

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Restart your NFS Server and docker services:

.. code:: sh

    $> sudo service nfs-kernel-server restart
    $> sudo service docker restart

On CentOS 7
^^^^^^^^^^^

Restart your NFS Server and docker daemons:

.. code:: sh

    $> sudo systemctl restart nfs-server
    $> sudo systemctl daemon-reload
    $> sudo systemctl restart docker

Set up your Docker Swarm service
--------------------------------

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We will now set up the Docker volumes that will be mounted to the NFS
Server and serve as data and metadata storage for S3 Server. These two
commands have to be replicated on all machines:

.. code:: sh

    $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/data --name data
    $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/metadata --name metadata

There is no need to ""docker exec" these volumes to mount them: the
Docker Swarm manager will do it when the Docker service will be started.

On Server
^^^^^^^^^

To start a Docker service on a Docker Swarm cluster, you first have to
initialize that cluster (i.e.: define a manager), then have the
workers/nodes join in, and then start the service. Initialize the swarm
cluster, and look at the response:

.. code:: sh

    $> docker swarm init --advertise-addr 10.200.15.113

    Swarm initialized: current node (db2aqfu3bzfzzs9b1kfeaglmq) is now a manager.

    To add a worker to this swarm, run the following command:

        docker swarm join \
        --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka \
        10.200.15.113:2377

    To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.

On Clients
^^^^^^^^^^

Simply copy/paste the command provided by your docker swarm init. When
all goes well, you'll get something like this:

.. code:: sh

    $> docker swarm join --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka 10.200.15.113:2377

    This node joined a swarm as a worker.

On Server
^^^^^^^^^

Start the service on your swarm cluster!

.. code:: sh

    $> docker service create --name s3 --replicas 1 --mount type=volume,source=data,target=/usr/src/app/localData --mount type=volume,source=metadata,target=/usr/src/app/localMetadata -p 8000:8000 scality/s3server

If you run a docker service ls, you should have the following output:

.. code:: sh

    $> docker service ls
    ID            NAME  MODE        REPLICAS  IMAGE
    ocmggza412ft  s3    replicated  1/1       scality/s3server:latest

If your service won't start, consider disabling apparmor/SELinux.

Testing your High Availability S3Server
---------------------------------------

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Try to find out where your Scality S3 Server is actually running using
the **docker ps** command. It can be on any node of the swarm cluster,
manager or worker. When you find it, you can kill it, with **docker stop
<container id>** and you'll see it respawn on a different node of the
swarm cluster. Now you see, if one of your servers falls, or if docker
stops unexpectedly, your end user will still be able to access your
local S3 Server.

Troubleshooting
---------------

To troubleshoot the service you can run:

.. code:: sh

    $> docker service ps s3docker service ps s3
    ID                         NAME      IMAGE             NODE                               DESIRED STATE  CURRENT STATE       ERROR
    0ar81cw4lvv8chafm8pw48wbc  s3.1      scality/s3server  localhost.localdomain.localdomain  Running        Running 7 days ago
    cvmf3j3bz8w6r4h0lf3pxo6eu   \_ s3.1  scality/s3server  localhost.localdomain.localdomain  Shutdown       Failed 7 days ago   "task: non-zero exit (137)"

If the error is truncated it is possible to have a more detailed view of
the error by inspecting the docker task ID:

.. code:: sh

    $> docker inspect cvmf3j3bz8w6r4h0lf3pxo6eu

Off you go!
-----------

Let us know what you use this functionality for, and if you'd like any
specific developments around it. Or, even better: come and contribute to
our `Github repository <https://github.com/scality/s3/>`__! We look
forward to meeting you!
