.. _old_sections/ad_inst_pre_cloudera:

.. _old_ad_sections/ad_inst_pre_cloudera:

.. _ad_inst_pre_cloudera:

==============================
Physical Machine Configuration
==============================

.. contents:: Table of Contents
    :local:
    :backlinks: none

The following tutorial goes through configuring a new physical machine
from scratch.
The first section, called "Pre-Configuration", is a little vague because
the information can vary between machines.
The second section, called "Base Configuration", will usually be identical for
all machines.

-----------------
Pre-Configuration
-----------------

1. Configure basic network connectivity.
#. Turn off IPV6 (many different ways to do this).
#. If applicable, mount any large volume to /mnt* (hint: vi /etc/fstab).

------------------
Base Configuration
------------------

1. Client-side |DNS|
====================
All systems in the cluster must be reachable by |DNS|
or by data in '/etc/hosts'.

2. Firewall
===========
The firewall must be disabled::

    service iptables stop
    chkconfig iptables off

3. SELINUX
==========
SELINUX must be disabled::

    vi /etc/selinux/config

Change::

    SELINUX=enforcing

To::

    SELINUX=disabled

Or::

    SELINUX=permissive

.. note::

    Systems will need full reboot before changes take effect.

4. Default Repositories
=======================
System default yum repos must be functional.

.. index::
    single: proxy

5. Proxy Settings
=================
If working behind a proxy, system proxy settings must be configured.

6. Syncronize System Packages
=============================
System packages must be syncronized with default repositories::

    yum clean all
    yum distro-sync

7. Primary |CDH| User
=====================
Cloudera supports use of root or sudo user as administration user.
If using sudo, user must have full nopassword sudo privileges.

8. *ssh* Connections
====================
Using the primary |CDH| user, every system in the cluster must be able to
communicate via *ssh* to all other systems in the cluster.

9. System Hostname
==================
Set the hostname for each system in the cluster.

.. note::

    Limiting host names to lower-case alphanumeric characters is recommended.

10. Ulimits
===========
The following definitions must exist in /etc/security/limits.conf
::

    vi /etc/security/limits.conf

    *                soft    nofile          32768
    *                hard    nofile          32768
    hadoop           -       nofile          32768
    hadoop           -       nproc           unlimited
    hdfs             -       nofile          32768
    hbase            -       nofile          32768
    spark            soft    nofile          65535
    spark            hard    nofile          65535
    spark            -       nproc           32768


11. NTP
=======
NTP must be installed and properly configured on all cluster systems.
Also NTP services should start on system boot::

    service ntpd start
    chkconfig ntpd on

All systems in cluster must be in time-sync with one-another.

12. Reboot
==========
Once all configuration has been done, rebooting all cluster systems is
recommended to properly activate all of the changes.
