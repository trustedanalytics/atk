.. _old_sections/ad_bkm:

.. _old_ad_sections/ad_bkm:

.. _ad_bkm:

==========================
Best Known Methods (Admin)
==========================

.. contents:: Table of Contents
    :local:
    :backlinks: none

-------------------------
Configuration information
-------------------------

.. toctree::
    :maxdepth: 1

    ad_cluster_configuration
    ad_gitune
    ad_hbtune

.. Outdated 20150728::

    .. index::
        single: Spark

    -----
    Spark
    -----

    .. index::
        single: disk full
        single: Red Hat
        single: CentOS
        single: Fedora

    Resolving disk full issue while running Spark jobs
    ==================================================

    Situation: On a Red Hat or an CentOS cluster, while running spark jobs, the
    /tmp drive becomes full and causes the jobs to fail.

    1)  Stop the trustedanalytics service::

        sudo service trustedanalytics stop

    #)  From |CDH| Web UI: first stop "Cloudera Management Service",
        and then stop the |CDH|.
    #)  Now run the following steps on each node:

        a)  Find the largest partition::

                df -h

        #)  Assuming /mnt is the largest partition, create the folder
            "/mnt/.bda/tmp", if it isn't already present::

                sudo mkdir -p /mnt/.bda/tmp

        #)  Set the permissions on this directory for total access::

                sudo chmod 1777 /mnt/.bda/tmp

        #)  Add the following line to the /etc/fstab file and save it::

                /mnt/.bda/tmp    /tmp    none   bind   0   0

        #)  Reboot the machine

    #)  After all the nodes are rebooted, from |CDH| Web UI: first start "Cloudera
        Management Service", and then start the |CDH|.

    Spark space concerns
    ====================

    Whenever a Spark application is run, redundant jars and logs go to
    /va/run/spark/work (or other location configured in Cloudera Manager).
    These can occupy over 140MB per command.

    * Short-term workarounds:

        *   Periodically delete these files
        *   Create a cron job to delete these files on a periodic basis.
            An example where the files are deleted eveyday at 2 am is::

                00 02 * * * sudo rm -rf /var/run/spark/work/app*

    * Long-term fix:

        *   Spark 1.0 will automatically clean up the files

    .. include:: ad_how.inc

    ----------
    References
    ----------

    `Spark Docs <https://spark.apache.org/documentation.html>`__

