.. _ad_tap:

===============================
Deploy and Run |ATK| App on TAP
===============================

----------------------------
Installing Required Packages
----------------------------

Install Golang from the package manager 
=========================================
On RedHat/CentOS, ensure "EPEL" repo is enabled.
For more information, see :ref:`Yum Repo <EPEL Repository>`.

From the command line interface (terminal),
install Go and the required libraries.

.. code::

    $ sudo yum install golang

To read more about Go, see https://golang.org/ .
To test the Go installation:

.. code::

    $ go version
    go version go1.4.2 linux/amd64

Install the CloudFoundry |CLI| package:

.. code::
   
    $ wget --content-disposition https://cli.run.pivotal.io/stable?release=redhat64

This command downloads the prepackaged RPM to your local machine.
Install this package:

.. code::

    $ sudo yum install cf-cli_amd64.rpm

.. note::

    See https://github.com/cloudfoundry/cli/releases for installation on a system not running RedHat/CentOS.

Test the package installation:

.. code::

    $ cf --version
    cf version 6.11.2-2a26d55-2015-04-27T21:11:44+00:00

Setting Up CF for |ATK| Deployment
==================================

First, add the <CLOUD_FOUNDRY_API_ENDPOINT> to the "no_proxy" list.

Now run ``cf api <CLOUD_FOUNDRY_API_ENDPOINT> -skip-ssl-validation`` to set your API endpoint.
You should see a message like this\:

.. code::

    $ cf api my_endpoint -skip-ssl-validation
    Setting api endpoint to my_endpoint...
    OK
    API endpoint: my_endpoint (API version: 2.25.0)
    Not logged in. Use 'cf login' to log in.

Now try login by running the command ``cf login -u <CLOUD_FOUNDRY_USERNAME> -p <CLOUD_FOUNDRY_PASSWORD>  -o <CLOUD_FOUNDRY_ORGANIZATION > -s <CLOUD_FOUNDRY_SPACE>``

Your output should look something like this:

.. code::

    $ cf login -u my_name -p my_password -o my_org -s my_space
    API endpoint: my_endpoint
    Authenticating...
    OK
    Targeted org my_org
    Targeted space my_space
    API endpoint: my_endpoint (API version: 2.25.0)
    User: my_name
    Org: my_org
    Space: my_space

Verify that you are still connected by running "cf target"
And your output looks like this:

.. code::

    $ cf target
    API endpoint: my_endpoint (API version: 2.25.0)
    User: my_name
    Org: my_org
    Space: my_space

Prepare |ATK| Tarball
=====================

For Admin:

|ATK| tarballs are built as part of the TeamCity build and are uploaded to S3.
In order to download the file, simply run the command:

.. code::

    wget https://s3.amazonaws.com/gao-internal-archive/<Your_Branch_Name>/trustedanalytics.tar.gz

For example, if you are on "master" branch you run:

.. code::

    wget https://s3.amazonaws.com/gao-internal-archive/master/trustedanalytics.tar.gz

For Dev:

You can build |ATK| tarball from scratch yourself.
In order to do so, do the following:

#)  CD to directory where the "atk" code is checked out.
#)  Build the atk code using Maven tool.
    Details for this change frequently, so please look at other Wiki pages like this one: Maven build
#)  CD to "package" directory and from there run this script:
    "config/trustedanalytics-rest-server-tar/package.sh".
    This creates a tar file like "atk.tar.gz" in the current directory.
#)  Deploy ATK to TAP (Ireland instance):
    Create a directory anywhere on your system, for example at "~/vcap/app" and
    unpack your "trustedanalytics.tar.gz" inside that directory.
#)  CD to "~/vcap/app" and create a file "manifest.yml" with this content:
    (For now please ensure you are using below memory and disk_quota values and
    do not change them)

    .. code::

        applications:
        - name: <YOUR_ATK_APP_NAME_HERE> for example "atk-yourapp"
          command: bin/rest-server.sh
          memory: 1G
          disk_quota: 2G
          timeout: 180
          instances: 1
        services:
        - bryn-cdh
        - <YOUR_POSTGRESQL_SERVICE_NAME_HERE> for example "pg-atk-yourapp"
        - bryn-zk
        env:
          CC_URI: <CLOUD_FOUNDRY_API_ENDPOINT> 
          UAA_URI: <UAA_ENDPOINT> 
          UAA_CLIENT_NAME: atk-client
          UAA_CLIENT_PASSWORD: *******

#)  Create an instance of PostgreSQL by running the command: 

    .. code::

        $ cf create-service postgresql93 free <PG-ATK-YOURAPP>

    and you should see an output like this:

    .. code::

        Creating service instance PG-ATK-YOURAPP in org seedorg / space seedspace as admin...
        OK

#)  Change to the "~/vcap/app" folder (or wherever you have
    "trustedanalytics.tar.gz" unpacked).
#)  Now run the command ``cf push``.
    This takes a few minutes to run and you should see the following output:

    .. code::

        [hadoop@master app]$ cf push
        Using manifest file /home/hadoop/vcap/app/manifest.yaml
        Creating app <ATK-YOURAPP> in org seedorg / space seedspace as admin...
        OK
        Using route yourdomain.for.yourapp.com
        Binding yourdomain.for.yourapp.com to <ATK-YOURAPP>...
        OK
        Uploading <ATK-YOURAPP>...
        Uploading app files from: /home/hadoop/vcap/app
        Uploading 48.3K, 9 files
        Done uploading
        OK
        Starting app <ATK-YOURAPP> in org seedorg / space seedspace as admin...
        0 of 1 instances running, 1 starting
        1 of 1 instances running
        App started

        OK
        App <ATK-YOURAPP> was started using this command `bin/rest-server.sh`
        Showing health and status for app <ATK-YOURAPP> in org seedorg / space seedspace as admin...
        OK
        requested state: started
        instances: 1/1
        usage: 1G x 1 instances
        urls: yourdomain.for.yourapp.com
        last uploaded: Wed May 20 22:22:54 UTC 2015
        stack: cflinuxfs2
        state since cpu memory disk details
        #0 running 2015-05-20 03:25:13 PM 0.0% 622.9M of 1G 432.9M of 2G

    If you like to see the complete configuration for your app, run the
    command "cf env <ATK-YOURAPP>".
#)  Retrieve data from VCAP_APPLICATION URIs.
#)  Create a client credentials file.
    For more information,
    see `Python Client Documentation <https://github.com/trustedanalytics/atk/wiki/python-client/>`__
#)  To tail your app logs:

    .. code::
       
        cf logs <ATK-YOURAPP>

#)  Open a Python2.7 or IPython session and do the following:

    .. code::

        In [1]: import trustedanalytics as ta
        In [2]: ta.connect("<PATH_TO_YOUR_CREDENTIALS_FILE")
        Connected to trustedanalytics server.
        In [3]: ta.server.host
        Out[3]: 'yourdomain.for.yourapp.com'
        In [4]: exit

