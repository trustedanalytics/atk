.. _ad_inst_ta1:

.. index::
    single: installation

====================
Package Installation
====================

.. contents::
    :local:
    :backlinks: none

------------
Introduction
------------

This guide covers |PACKAGE| installation and configuration.

Cloudera installation documentation can be found at:
http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM5/latest/Cloudera-Manager-Installation-Guide/cm5ig_install_cm_cdh.html .

------------
Requirements
------------

Operating System Requirements
=============================

These instructions are oriented towards `Red Hat Enterprise Linux
<http://redhat.com/>`__ or `CentOS <http://centos.org/>`__ version 6.6.
|PACKAGE| uses 'yum' for installation, 'sudo' for proper authority.

.. index::
    single: cluster

Cluster Requirements
====================

Cloudera cluster 5.3.x with following services:

#.  HDFS
#.  SPARK
#.  Hbase
#.  Yarn(MR2)
#.  Zookeeper

|PACKAGE| Python client supports Python 2.7.

----------------------
|TOOLKIT| Installation
----------------------

.. index::
    single: repository

Adding Extra Repositories
=========================

The EPEL and |PACKAGE| repositories must be installed on the REST server node and
all spark nodes (master and worker).
The |PACKAGE| Dependency repository and the yum-s3 package must be installed before
the |PACKAGE| private repository.

.. _epel repository:

EPEL Repository
---------------

Verify the installation of the "epel" repository:

..code::

    $ sudo yum repolist

.. only:: html

    Sample output::

        repo id                                    repo name
        cloudera-cdh5                              Cloudera Hadoop, Version 5                                           141
        cloudera-manager                           Cloudera Manager, Version 5.3.1                                        7
        epel                                       Extra Packages for Enterprise Linux 6 - x86_64                    11,022
        rhui-REGION-client-config-server-6         Red Hat Update Infrastructure 2.0 Client Configuration Server 6        2
        rhui-REGION-rhel-server-releases           Red Hat Enterprise Linux Server 6 (RPMs)                          12,690
        rhui-REGION-rhel-server-releases-optional  Red Hat Enterprise Linux Server 6 Optional (RPMs)                  7,168

.. only:: latex

    Sample output::

        repo id                           repo name
        cloudera-cdh5                     Cloudera Hadoop, Version 5            ...
        cloudera-manager                  Cloudera Manager, Version 5.3.1       ...
        epel                              Extra Packages for Enterprise Linux 6 ...
        rhui-REGION-client-config-ser...  Red Hat Update Infrastructure 2.0 Clie...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 (RPM...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 Opti...

If the "epel" repository is not listed, do this to install it:

.. only:: html

    ..code::

        $ wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
        $ sudo rpm -ivh epel-release-6-8.noarch.rpm

.. only:: latex

    .. code::

        $ wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.
            noarch.rpm
        $ sudo rpm -ivh epel-release-6-8.noarch.rpm

.. index::
    single: repository

|PACKAGE| Dependency Repository
-------------------------------

Some open source libraries are included to aid with the installation of the
|PACKAGE|.
Some of these libraries are newer versions than what is available in RHEL,
EPEL or CentOS repositories.

To add the dependency repository, do this:

.. only:: html

    .. code::

        $ wget https://trustedanalytics-dependencies.s3-us-west-2.amazonaws.com/ta-deps.repo
        $ sudo cp ta-deps.repo /etc/yum.repos.d/

.. only:: latex

    .. code::

        $ wget https://trustedanalytics-dependencies.s3-us-west-2.amazonaws.com/
            ta-deps.repo
        $ sudo cp ta-deps.repo /etc/yum.repos.d/

Alternatively, do this to build the dependency repository information file
directly:

.. code::

    $ echo "[trustedanalytics-deps]
    > name=trustedanalytics-deps
    > baseurl=https://trustedanalytics-dependencies.s3-us-west-2.amazonaws.com/yum
    > gpgcheck=0
    > priority=1 enabled=1"  | sudo tee -a /etc/yum.repos.d/ta-deps.repo

.. only:: html

    This code is :download:`downloadable <_downloads/ta-deps.sh>` (open, copy, and paste).

Test the installation of the dependencies repository:

.. code::

    $ sudo yum info yum-s3

Results should be similar to this::

    Available Packages
    Name        : yum-s3
    Arch        : noarch
    Version     : 0.2.4
    Release     : 1
    Size        : 9.0 k
    Repo        : trustedanalytics-deps
    Summary     : Amazon S3 plugin for yum.
    URL         : git@github.com:NumberFour/yum-s3-plugin.git
    License     : Apache License 2.0

Installing the *yum-s3* package allows access to the Amazon S3 repository.
To install the *yum-s3* package, do this:

.. code::

    $ sudo yum -y install yum-s3


.. _add_tA_private_repository:

.. index::
    single: repository

|PACKAGE| Private Repository
----------------------------

Create '/etc/yum.repos.d/ta.repo':

.. only:: html

    .. code::

        $ echo "[trustedanalytics]
        > name=trustedanalytics
        > baseurl=https://trustedanalytics-repo.s3-us-west-2.amazonaws.com/release/latest/yum/dists/rhel/6
        > gpgcheck=0
        > priority=1
        > s3_enabled=1
        > key_id=ACCESS_TOKEN
        > secret_key=SECRET_TOKEN" | sudo tee -a /etc/yum.repos.d/ta.repo


    This code is :download:`downloadable <_downloads/ta-repo.sh>` (open, copy, and paste).

.. only:: latex

    .. code::

        $ echo "[trustedanalytics]
        > name=trustedanalytics
        > baseurl=https://trustedanalytics-repo.s3-us-west-2.amazonaws.com/
            release/latest/yum/dists/rhel/6
        > gpgcheck=0
        > priority=1
        > s3_enabled=1
        > key_id=ACCESS_TOKEN
        > secret_key=SECRET_TOKEN" | sudo tee -a /etc/yum.repos.d/ta.repo

    Note: baseurl line above is broken for readability.
    It should be entered as a single line.

.. note::

    Replace "ACCESS_TOKEN" and "SECRET_TOKEN" with appropriate tokens.

To verify the installation of the |PACKAGE| repository, do this:

.. code::

    $ sudo yum info trustedanalytics-rest-server

Example results::

    Available Packages
    Name        : trustedanalytics-rest-server
    Arch        : x86_64
    Version     : #.#.#
    Release     : ####
    Size        : 419 M
    Repo        : trustedanalytics
    Summary     : trustedanalytics-rest-server-0.9
    URL         : trustedanalytics.com
    License     : Confidential

Troubleshooting Private Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*   The most common errors when using the private repository:

    *   Incorrect access token/key
    *   Incorect secret token/key
    *   The server time is out of sync with the world

*   Double check the access and secret keys in the ta.repo file.
*   AWS S3 will fail with access denied errors if the system time is out of
    sync with the website.
    To keep the system time in sync with the website run:

    .. code::

        $ sudo service ntpd start


*   The |PACKAGE| Dependency repository and the yum-s3 package must be installed
    before the |PACKAGE| private repository.
*   To use the yum command inside a corporate proxy make sure the
    *http_proxy* and *https_proxy* environment variables are set.
*   The sudo command may need the -E option to maintain environment variables:

    .. code::

        $ sudo -E yum command

.. _installing_tA_packages:

Installing |TOOLKIT|
====================

Installing On The Master Node
-----------------------------

Install |PACKAGE| Python REST server and its dependencies.
Only one instance of the REST server needs to be installed.
Installation location is flexible, but it is usually installed
with the HDFS name node.

.. code::

    $ sudo yum -y install trustedanalytics-rest-server

Installing On A Worker Node
---------------------------

The |PACKAGE| spark dependencies package needs to be installed on every node
running the spark worker role.

.. only:: html

    .. code::

        $ sudo yum -y install trustedanalytics-spark-deps trustedanalytics-python-rest-client

.. only:: latex

    .. code::

        $ sudo yum -y install trustedanalytics-spark-deps
        $ sudo yum -y install trustedanalytics-python-rest-client


.. index::
    single: REST server

Starting The |PACKAGE| REST Server
==================================

Starting the REST server is very easy.
It can be started like any other Linux service.

.. code::

    $ sudo service trustedanalytics start

After starting the REST server, browse to the host on port 9099
(<master node ip address>:9099) to see if the server started successfully.

Troubleshooting |PACKAGE| REST Server
=====================================

A log gets written to '/var/log/trustedanalytics/rest-server/output.log or
'/var/log/trustedanalytics/rest-server/application.log'.
To resolve issues starting or running jobs, tail either log to see what
error is getting reported while running the task:

.. code::

    $ sudo tail -f /var/log/trustedanalytics/rest-server/output.log

or:

.. code::

    $ sudo tail -f /var/log/trustedanalytics/rest-server/application.log



Upgrading
=========

Unless specified otherwise in the release notes, upgrading requires removal of
old software prior to installation of new software.

.. toctree::
    :hidden:

    ad_inst_ta3

