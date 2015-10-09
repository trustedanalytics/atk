.. index:: ! How to ...

----------
How To ...
----------

.. contents:: Table of Contents
    :local:
    :backlinks: none

How to start Cloudera Manager
=============================
1.  Open a browser.
#.  Go to address ``http://<ipaddressofmachine>:7180``.
#.  Default username and password are both *admin*.

How to transfer data files
==========================
1.  Open terminal session.
#.  Enter ``ssh <user id>@<ipaddressofmachine>``.
#.  Enter ``hadoop fs -put <localfileaddress> <hdfsfileaddress>``

How to map a local directory to cluster
=======================================

To set up
---------
1.  Open terminal session on local machine.
#.  Create a directory to use as the transfer.
    For example::

        mkdir /home/common

#.  Enter ``sshfs <user id>@<ipaddressofmachine>:<remotedirectory>
    <localdirectory>``.
    For example::

      sshfs hadoop@10.54.10.110:/home/ /home/common

To close down
-------------
1.  Open terminal session on local machine.
#.  Enter ``fusermount -u <localdirectory>``.
    For example::

      fusermount -u /home/common

