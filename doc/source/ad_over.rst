.. _ad_over:

=============================
Deploy and Run ATK App on DP2
=============================

----------------------------
Installing Required Packages
----------------------------

Install "golang" from the package manager 
=========================================
On RedHat/CentOS, ensure "EPEL" repo is enabled.
For more information, see :ref:`Yum Repo <ad_yum_epel_repo>`.

From the command line interface (terminal),
install the "go" language and the required libraries.

.. code::

    $ sudo yum install golang

To read more about "go" see https://golang.org/ .
To test the "go" installation, run the command ``go``.
The response should be similar to:

.. code::

    <show what it looks like>

Install the CloudFoundry |CLI| package:

.. code::
   
    $ wget --content-disposition https://cli.run.pivotal.io/stable?release=redhat64

This downloads the prepackaged RPM to your local machine.
Install this package:

.. code::

    $ sudo yum install cf-cli_amd64.rpm

.. note::

    See https://github.com/cloudfoundry/cli/releases for installation on a system not running RedHat/CentOS.

Test the package installation:

.. code::
   
    $ cf
    < put response here>

Setting up CF for ATK deployment (Ireland instance):
First run cf api https://api.run.gotapaas.eu to set your API endpoint.
You should see a message like this:
[hadoop@master ~]$ cf api https://api.run.gotapaas.eu --skip-ssl-validation
Setting api endpoint to https://api.run.gotapaas.eu...
OK

API endpoint: https://api.run.gotapaas.eu (API version: 2.25.0)
Not logged in. Use 'cf login' to log in.
Now try login by running the command "cf login -u admin -p c1oudc0w -o seedorg -s seedspace":
Your output should look something like this:

.. code::

    [hadoop@master ~]$ cf login -u admin -p c1oudc0w -o seedorg -s seedspace
    API endpoint: https://api.run.gotapaas.eu
    Authenticating...
    OK
    Targeted org seedorg
    Targeted space seedspace
    API endpoint: https://api.run.gotapaas.eu (API version: 2.25.0)
    User: admin
    Org: seedorg
    Space: seedspace

Verify that you are still connected by running "cf target"
And your output looks like this:

.. code::

    [hadoop@master ~]$ cf target
    API endpoint: https://api.run.gotapaas.eu (API version: 2.25.0)
    User: admin
    Org: seedorg
    Space: seedspace
    TBD

Prepare ATK tarball:

For QA:

ATK tarballs are built as part of the TeamCity build and are uploaded to S3.
In order to download the file, simply run the command:

.. code::

    wget https://s3.amazonaws.com/gao-internal-archive/<Your_Branch_Name>/trustedanalytics.tar.gz

For example, if you are on "master" branch you run:

.. code::

    wget https://s3.amazonaws.com/gao-internal-archive/master/trustedanalytics.tar.gz

For Dev:

You can build ATK tarball from scratch yourself.
In order to do so, do the following:

#)  CD to directory where your have the "atk" code checked out.
#)  Build the atk code using Maven tool.
    Details for this change frequently, so please look at other Wiki pages like this one: Maven build
#)  CD to "package" directory and from there run this script:
    "config/trustedanalytics-rest-server-tar/package.sh".
    This creates a tar file like "atk.tar.gz" in the current directory.
#)  Deploy ATK to DP2 (Ireland instance):
    Create a directory anywhere on your system, for example at "~/vcap/app" and
    unpack your "trustedanalytics.tar.gz" inside that directory.
#)  CD to "~/vcap/app" and create a file "manifest.yml" with this content:
    (For now please ensure you are using below memory and disk_quota values and
    do not change them)

    .. code::

        applications:
        - name: <YOUR_ATK_APP_NAME_HERE> for example "atk-ebi"
          command: bin/rest-server.sh
          memory: 1G
          disk_quota: 2G
          timeout: 180
          instances: 1
        services:
        - bryn-cdh
        - <YOUR_POSTGRESQL_SERVICE_NAME_HERE> for example "pg-atk-ebi"
        - bryn-zk

#)  Create an instance of PostgreSQL by running the command: 

    .. code::

        $ cf create-service postgresql93 free pg-atk-ebi

    and you should see an output like this:

    .. code::

        Creating service instance pg-atk-ebi in org seedorg / space seedspace as admin...
        OK

#)  Change conf/application.conf, making sure "fs.root" is set to:

    .. code::
       
        fs.root = ${FS_ROOT}"/"${APP_NAME}

#)  Change to the "~/vcap/app" folder (or wherever you have
    "trustedanalytics.tar.gz" unpacked).
#)  Now run the command ``cf push``.
    This takes a few minutes to run and you should see the following output:

    .. code::

        [hadoop@master app]$ cf push
        Using manifest file /home/hadoop/vcap/app/manifest.yaml
        Creating app atk-ebi in org seedorg / space seedspace as admin...
        OK
        Using route atk-ebi.apps.gotapaas.eu
        Binding atk-ebi.apps.gotapaas.eu to atk-ebi...
        OK
        Uploading atk-ebi...
        Uploading app files from: /home/hadoop/vcap/app
        Uploading 48.3K, 9 files
        Done uploading
        OK
        Binding service bryn-cdh to app atk-ebi in org seedorg / space seedspace as admin...
        OK
        Binding service pg-atk-ebi to app atk-ebi in org seedorg / space seedspace as admin...
        OK
        Binding service bryn-zk to app atk-ebi in org seedorg / space seedspace as admin...
        OK
        Starting app atk-ebi in org seedorg / space seedspace as admin...
        0 of 1 instances running, 1 starting
        1 of 1 instances running
        App started

        OK
        App atk-ebi was started using this command `bin/rest-server.sh`
        Showing health and status for app atk-ebi in org seedorg / space seedspace as admin...
        OK
        requested state: started
        instances: 1/1
        usage: 1G x 1 instances
        urls: atk-ebi.apps.gotapaas.eu
        last uploaded: Wed May 20 22:22:54 UTC 2015
        stack: cflinuxfs2
        state since cpu memory disk details
        #0 running 2015-05-20 03:25:13 PM 0.0% 622.9M of 1G 432.9M of 2G

    If you like to see the complete configuration for your app, run the
    command "cf env atk-ebi".
#)  Retrieve data from VCAP_APPLICATION uris.
#)  Create a client credentials file.
    For more information,
    see https://github.com/trustedanalytics/atk/wiki/python-client
#)  To tail your app logs:

    .. code::
       
        cf logs atk-ebi

#)  Open a Python2.7 or IPython session and do the following:

    .. code::

        In [1]: import trustedanalytics as atk
        In [2]: atk.connect("<PATH_TO_YOUR_CREDENTIALS_FILE")
        Connected to intelanalytics server.
        In [3]: atk.server.host
        Out[3]: 'atk-ebi.apps.gotapaas.eu'
        In [4]: exit

#)  Ready to run some examples:

    .. code::

        TBD
