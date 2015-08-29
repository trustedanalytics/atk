================
Virtual Machines
================

.. index::
    pair: virtual; machine

.. contents:: Table of Contents
    :local:
    :backlinks: none

------------
Introduction
------------

This guide goes through the download and import of the |PACKAGE| beta on a virtual
machine (VM).
Currently the |PACKAGE| |VM| only supports
`Virtual Box <https://www.virtualbox.org/>`_.
These instructions do not cover the installation of Virtual Box.
Virtual Box supports many platforms and can be `downloaded for free
<https://www.virtualbox.org/wiki/Downloads>`_.
The installation documentation is also
`available online <https://www.virtualbox.org/manual/UserManual.html>`_.

------------
Requirements
------------

*   12GB of memory needs to be allocated to the |VM|
*   45GB of free hard drive space
*   Working Virtual Box 4.3 installation

-------------------
Download |VM| Image
-------------------

Open a Linux shell (or for Windows user a command prompt) to run the
various commands.

The |VM| image is downloaded from AWS.
The download requires the AWS Command Line Interface (CLI) client.
Instructions for downloading and installing CLI can be found at
`Amazon cli documentation
<http://docs.aws.amazon.com/cli/latest/userguide/installing.html>`_.

After installing the interface, verify the installation by running::

    $ aws --version

The result should be similar to this::

    aws-cli/1.2.9 Python/2.7.6 Linux/3.8.0-35-generic

Take note of the aws-cli version, as it must be greater or equal to 1.2.9.
Older versions of the aws-cli client does not work with the restricted
permissions.

The aws-cli client can be updated with pip or
download the new Windows MSI (reference :ref:`Windows GUI Client`).
::

    $ sudo pip install -U awscli

After aws installation, run::

    $ aws configure

The program prompts for the access and secret tokens given at registration.
When prompted for the "Default region name", use "us-west-2".
When prompted for the "Default output format", use "json".
::

    AWS Access Key ID [None]: <my access key>
    AWS Secret Access Key [None]: <my secret key>
    Default region name [None]: <us-west-2>
    Default output format [None]: <json>

List the files in the directory::

    $ aws s3 ls s3://trustedanalytics-repo/release/latest/vm/
    2014-08-19 12:57:03           0
    2014-11-25 16:22:57          70 TrustedAnalytics-VM.md5
    2014-11-25 16:22:57 14656025025 TrustedAnalytics-VM.tar.gz

Download the tar.gz file.
In this case, it's 'TrustedAnalytics-VM.tar.gz'::

    $ aws s3 cp s3://trustedanalytics-repo/release/latest/vm/TrustedAnalytics-VM.tar.gz ./

[:ref:`Skip section about Windows GUI Client <extract_archive>`].

.. _windows gui client:

------------------
Windows GUI Client
------------------
If you are on a Windows machine, and you prefer a GUI client, use the
`S3Browser <http://s3browser.com/>`__ to download the |VM|.

1)  Download the Windows MSI http://s3browser.com/download.php.
#)  Install and open the S3Browser application.
#)  Add the keys provided.

    A)  Navigate to:

        1)  **Accounts**
        #)  **Add new account**

        or press **Ctrl** + **Shift** + **A**.

        See :ref:`Fig. 15.1 <fig_15_01>`.

        .. _fig_15_01:

        .. only:: html

            .. figure:: ad_inst_vm_add_new_acct.*
                :width: 60%
                :align: center

                Fig. 15.1
                Add New Account

        .. only:: latex

            .. figure:: ad_inst_vm_add_new_acct.*
                :align: center

                Add New Account

    #)  In the account creation window:

        1)  Add your access and secret keys
        #)  Give the account a name

        See :ref:`Fig. 15.2 <fig_15_02>`.

        .. _fig_15_02:

        .. only:: html

            .. figure:: ad_inst_vm_new_acct_info.*
                :width: 60%
                :align: center

                Fig. 15.2
                New Account Information

        .. only:: latex

            .. figure:: ad_inst_vm_new_acct_info.*
                :align: center

                New Account Information

#)  Navigate to:

    A)  **Buckets**
    #)  **Add External Bucket**

    or press **Ctrl** + **E**.

    See :ref:`Fig. 15.3 <fig_15_03>`.

    .. _fig_15_03:

    .. only:: html

        .. figure:: ad_inst_vm_add_bucket.*
            :width: 60%
            :align: center

            Fig. 15.3
            Add External Bucket

    .. only:: latex

        .. figure:: ad_inst_vm_add_bucket.*
            :align: center

            Add External Bucket

#)  Add the bucket url "trustedanalytics-repo/release",
    then click **Add External bucket**.
    See :ref:`Fig. 15.4 <fig_15_04>`.

    .. _fig_15_04:

    .. only:: html

        .. figure:: ad_inst_vm_bucket_name.*
            :width: 60%
            :align: center

            Fig. 15.4
            Give Bucket Name

    .. only:: latex

        .. figure:: ad_inst_vm_bucket_name.*
            :align: center

            Give Bucket Name

#)  After adding the bucket, a list of folders shows up on the right.
    See :ref:`Fig. 15.5 <fig_15_05>`.

    .. _fig_15_05:

    .. only:: html

        .. figure:: ad_inst_vm_check_folder_list.*
            :width: 60%
            :align: center

            Fig. 15.5
            Check Folder List

    .. only:: latex

        .. figure:: ad_inst_vm_check_folder_list.*
            :align: center

            Check Folder List

#)  Select the appropriate version, and navigate to the |VM| folder,
    then right click and download the "tar.gz" file.
    See :ref:`Fig. 15.6 <fig_15_06>`.

    .. _fig_15_06:

    .. only:: html

        .. figure:: ad_inst_vm_download_file.*
            :width: 60%
            :align: center

            Fig. 15.6
            Download File

    .. only:: latex

        .. figure:: ad_inst_vm_download_file.*
            :align: center

            Download File

.. _extract_archive:

---------------
Extract Archive
---------------

Extracting On Windows
=====================
Extracting on Windows is relatively easy.
Use `7zip <http://7-zip.org/>`_ (or equivalent tool) to extract the archive.

Extracting On Linux
===================
After acquiring the |VM|, extract the archive::

    $ tar -xvf TrustedAnalytics-VM.tar.gz

After extraction, there should be two (2) files,
one with the extension 'vmdk', and another with the extension 'ovf'.

------------
Import Image
------------
To import the |VM| image, do the following steps in Virtual Box.

1)  Go to the **File** menu, then **Import Appliance**.
    See :ref:`Fig. 15.7 <fig_15_07>`.

    .. _fig_15_07:

    .. only:: html

        .. figure:: ad_inst_vm_file_import_app.*
            :width: 60%
            :align: center

            Fig. 15.7
            File -> Import Appliance

    .. only:: latex

        .. figure:: ad_inst_vm_file_import_app.*
            :align: center

            File -> Import Appliance

#)  Select the file with the extension 'ovf', which was extracted earlier from
    the |VM| image.
    See :ref:`Fig. 15.8 <fig_15_08>`.

    .. _fig_15_08:

    .. only:: html

        .. figure:: ad_inst_vm_app_to_import.*
            :width: 60%
            :align: center

            Fig. 15.8
            Appliance to Import

    .. only:: latex

        .. figure:: ad_inst_vm_app_to_import.*
            :align: center

            Appliance to Import

#)  Import the |PACKAGE| |VM|.
    See :ref:`Fig. 15.9 <fig_15_09>`.

    .. _fig_15_09:

    .. only:: html

        .. figure:: ad_inst_vm_app_settings.*
            :width: 60%
            :align: center

            Fig. 15.9
            Appliance Settings

    .. only:: latex

        .. figure:: ad_inst_vm_app_settings.*
            :align: center

            Appliance Settings

#)  After clicking **Import**, wait for the |VM| to be imported.
    See :ref:`Fig. 15.10 <fig_15_10>`.

    .. _fig_15_10:

    .. only:: html

        .. figure:: ad_inst_vm_watch_import.*
            :width: 60%
            :align: center

            Fig. 15.10
            Watching Appliance Import

    .. only:: latex

        .. figure:: ad_inst_vm_watch_import.*
            :align: center

            Watching Appliance Import

#)  Once the |VM| is imported, boot the |VM| by selecting the |VM| and
    clicking **Start**.
    See :ref:`Fig. 15.11 <fig_15_11>`.

    .. _fig_15_11:

    .. only:: html

        .. figure:: ad_inst_vm_boot_vm.*
            :width: 60%
            :align: center

            Fig. 15.11
            Boot the VM

    .. only:: latex

        .. figure:: ad_inst_vm_boot_vm.*
            :align: center

            Boot the VM

------------------------
Running |PACKAGE| |VM| Image
------------------------

Before starting
===============

After every reboot of the |VM|, the |PACKAGE| server must also be restarted.
::

    $ sudo service trustedanalytics restart

Upon restart, if the service wasn't running before it was told to stop,
the system reports::

    initctl: Unknown instance:

This message can be safely ignored.


Sample Scripts
==============

The |VM| is pre-configured and installed with the |PACKAGE|.
Several examples and datasets are included to get people
familiar with the coding and behavior of the |PACKAGE|.

The examples are located in '/home/cloudera/examples'.
::

    drwxr-xr-x 2 cloudera cloudera 4096 Aug  1 00:53 datasets
    -rw-r--r-- 1 cloudera cloudera 1100 Aug  1 10:15 lbp.py
    -rw-r--r-- 1 cloudera cloudera  707 Aug  1 00:53 lda.py
    -rw-r--r-- 1 cloudera cloudera  930 Aug  1 00:53 lp.py

The datasets are located in '/home/cloudera/examples/datasets' and
'hdfs://user/trustedanalytics/datasets/'.
::

    -rw-r--r--   1 atkuser atkuser        122 2014-08-01 /user/trustedanalytics/datasets/README
    -rw-r--r--   1 atkuser atkuser     617816 2014-08-01 /user/trustedanalytics/datasets/apl.csv
    -rw-r--r--   1 atkuser atkuser    8162836 2014-08-01 /user/trustedanalytics/datasets/lbp_edge.csv
    -rw-r--r--   1 atkuser atkuser     188470 2014-08-01 /user/trustedanalytics/datasets/lp_edge.csv
    -rw-r--r--   1 atkuser atkuser  311641390 2014-08-01 /user/trustedanalytics/datasets/test_lda.csv

The datasets in '/home/cloudera/examples/datasets' are for reference.
The actual data that is being used by the Python examples and the |PACKAGE| server
is in 'hdfs://user/trustedanalytics/datasets'.

To run any of the Python example scripts, start in the examples directory and
start Python with the script name::

    $ python <SCRIPT_NAME>.py

where ``<SCRIPT_NAME>`` is any of the scripts in '/home/cloudera/example'.

Example::

    $ cd /home/cloudera/examples
    $ python pr.py

.. index::
    single: Eclipse
    single: PyDev

-------------
Eclipse/PyDev
-------------
The |VM| comes with Eclipse and PyDev installed and ready for use.
Importing the example scripts is easy.

1.  Go to the desktop, and double-click on the Eclipse icon.
#.  Go to **File** menu, and select **New** and then **Other**.

    See :ref:`Fig. 15.12 <fig_15_12>`.

    .. _fig_15_12:

    .. only:: html

        .. figure:: ad_inst_vm_start_eclipse.*
            :width: 60%
            :align: center

            Fig. 15.12
            Starting Eclipse

    .. only:: latex

        .. figure:: ad_inst_vm_start_eclipse.*
            :align: center

            Starting Eclipse

#.  After selecting **File**->**New**->**Other**, look for the PyDev folder
    and expand the list, then select **PyDev Project** then click **Next**.
    See :ref:`Fig. 15.13 <fig_15_13>`.

    .. _fig_15_13:

    .. only:: html

        .. figure:: ad_inst_vm_new_pydev.*
            :width: 60%
            :align: center

            Fig. 15.13
            New PyDev Project

    .. only:: latex

        .. figure:: ad_inst_vm_new_pydev.*
            :align: center

            New PyDev Project

#.  The only field you have to change is the 'Project Contents' default
    directory.
    Uncheck 'Use default' and enter the directory you want to use
    '/home/cloudera/examples'.
    Everything else can be left with the default values.
    Click **Next** when you are done.
    See :ref:`Fig. 15.14 <fig_15_14>`.

    .. _fig_15_14:

    .. only:: html

        .. figure:: ad_inst_vm_working_path.*
            :width: 60%
            :align: center

            Fig. 15.14
            Enter Working Path

    .. only:: latex

        .. figure:: ad_inst_vm_working_path.*
            :align: center

            Enter Working Path

#.  You should now be able to see all the example scripts on the left hand
    pane.
    See :ref:`Fig. 15.15 <fig_15_15>`.

    .. _fig_15_15:

    .. only:: html

        .. figure:: ad_inst_vm_example_scripts.*
            :width: 60%
            :align: center

            Fig. 15.15
            Examining Example Scripts

    .. only:: latex

        .. figure:: ad_inst_vm_example_scripts.*
            :align: center

            Examining Example Scripts

.. index::
    single: log

----
Logs
----

To debug changes to the scripts (or to peek behind the curtain), the log
file is '/var/log/trustedanalytics/rest-server/output.log'.
To show the log as it is generated, run ``tail -f``::

    $ sudo tail -f /var/log/trustedanalytics/rest-server/output.log

More details can be found in the :doc:`section on log files </ad_log>`.

--------
Updating
--------

Upon receipt of access and secret tokens, edit '/etc/yum.repos.d/atk.repo' and
replace *myKey* and *mySecret*.
Afterwards, it is recommended to run ``yum`` commands to check for and perform
updates.

.. only:: html

    ::

        $ sudo [vi|vim] /etc/yum.repos.d/atk.repo

        [Trusted Analytics repo]
        name=Trusted Analytics yum repo
        baseurl=https://s3-us-west-2.amazonaws.com/trustedanalytics-repo/release/latest/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        #enabled=0
        s3_enabled=0
        key_id=myKey
        secret_key=mySecret

.. only:: latex

    ::

        $ sudo [vi/vim] /etc/yum.repos.d/atk.repo

        [Trusted Analytics repo]
        name=Trusted Analytics yum repo
        baseurl=https://s3-us-west-2.amazonaws.com/trustedanalytics-repo/
            release/latest/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        #enabled=0
        s3_enabled=0
        key_id=myKey
        secret_key=mySecret

    The baseurl line shown above has been broken for proper display in certain
    media.
    It should be entered as a single line with no spaces.

To check for new updates and see the difference between the new and installed
version::

    $ sudo yum info trustedanalytics-rest-server

To update::

    $ sudo yum update trustedanalytics-rest-server

------------------
Common VM problems
------------------
*   The VM doesn't have enough memory allocated.
*   The TA REST server wasn't restarted after restart or boot.

