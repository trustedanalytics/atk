.. _ad_cluster_configuration:

=====================
Cluster Configuration
=====================

.. contents:: Table of Contents
    :local:
    :backlinks: none

.. index::
    single: cluster, configuration

The cluster configuration tool allows easy updating and optimization of any
|CDH| cluster from the command line.
With the use of optimization formulas you can have an optimized |CDH| cluster
in a matter of seconds.

The tool does 3 things:

*   Generate optimized configurations based on your cluster hardware.
*   Easy updating, restarting, and configuration deployment of |CDH| services.
*   Simple exploration of |CDH| cluster configurations.

------------
Installation
------------

Until the pip package is hosted, you will need to clone and install directly
from source.

.. code::

    git clone git@github.intel.com:gao/cluster-config.git
    cd cluster-config
    [sudo] python setup.py install 

Requirements
============

*   Python 2.7
*   `argparse <https://docs.python.org/2.7/library/argparse.html>`__ >= 1.3.0
*   `cm-api <https://github.com/cloudera/cm_api>`__ == 10.0.0
*   Working |CDH| cluster for running the script

----
Help
----

After installing, you will have three executable scripts on your path:
:ref:`cluster_generate`  cluster-config_, and cluster-explore_.

.. _common_options:

Common Options
==============

All three scripts have mostly overlapping configurations.

*   **--host**: |CDH| ip or host name, always required
*   **--port**: |CDH| port, default 7180, required if your |CDH| manager UI
    port is not 7180
*   **--username**: |CDH| username, default 'admin', required if your |CDH|
    manager UI username is not 'admin'
*   **--password**: |CDH| manager password, if not provided on the command
    line you will be asked at runtime
*   **--cluster**: |CDH| cluster name and the name of the cluster if more
    than one cluster is managed by cloudera manager.
*   **--path**: The path we will save and load files, like *cdh.json*,
    default is working directory
*   **--log**: Log level, defaults to INFO

Running any of the scripts with the --help option will provide help text for
all the command line options.

.. _help_cluster-generate:

Help |EM| cluster-generate
==========================

.. code::

    $ cluster-generate --help
    usage: cluster-generate [-h] [--formula FORMULA] [--formula-args FORMULA_ARGS]
                            --host HOST [--port PORT] [--username USERNAME]
                            [--password PASSWORD] [--cluster CLUSTER]
                            [--path PATH] [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

    Auto generate various CDH configurations based on system resources

    optional arguments:
      -h, --help            show this help message and exit
      --formula FORMULA     Auto generation formula file. Defaults to
                            /home/rodorad/Documents/config/atk-
                            config/cluster_config/formula.py
      --formula-args FORMULA_ARGS
                            Auto generation formula arguments to possibly override
                            global configurations.
      --host HOST           Cloudera Manager Host
      --port PORT           Cloudera Manager Port
      --username USERNAME   Cloudera Manager User Name
      --password PASSWORD   Cloudera Manager Password
      --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                            is managed by Cloudera Manager.
      --path PATH           Directory where we can save/load configurations files.
                            Defaults to working directory
                            /home/rodorad/Documents/config/atk-config
      --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                            Log level [INFO|DEBUG|WARNING|FATAL|ERROR]

.. _help_cluster-config:

Help |EM| cluster-config
========================

.. code::

    $  cluster-config --help
    usage: cluster-config [-h] --update-cdh {no,yes} --restart-cdh {no,yes}
                          [--conflict-merge {interactive,user,generated}] --host
                          HOST [--port PORT] [--username USERNAME]
                          [--password PASSWORD] [--cluster CLUSTER] [--path PATH]
                          [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

    Process cl arguments to avoid prompts in automation

    optional arguments:
      -h, --help            show this help message and exit
      --update-cdh {no,yes}
                            Should we update CDH with all configurations in
                            cdh.json/user-cdh.json?
      --restart-cdh {no,yes}
                            Should we restart CDH services after configuration
                            changes
      --conflict-merge {interactive,user,generated}
                            When encountering merge conflicts between the
                            generated configuration() and the user configuration()
                            what value should we default to? The 'user',
                            'generated', or 'interactive'resolution
      --host HOST           Cloudera Manager Host
      --port PORT           Cloudera Manager Port
      --username USERNAME   Cloudera Manager User Name
      --password PASSWORD   Cloudera Manager Password
      --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                            is managed by Cloudera Manager.
      --path PATH           Directory where we can save/load configurations files.
                            Defaults to working directory /home/SOME_USER
      --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                            Log level [INFO|DEBUG|WARNING|FATAL|ERROR]

.. _help_cluster-explore:

Help |EM| cluster-explore
=========================

.. code::

    $ cluster-explore --help
    usage: cluster-explore [-h] [--dump DUMP] --host HOST [--port PORT]
                           [--username USERNAME] [--password PASSWORD]
                           [--cluster CLUSTER] [--path PATH]
                           [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

    Process cl arguments to avoid prompts in automation

    optional arguments:
      -h, --help            show this help message and exit
      --dump DUMP           If you want to dump all configs without asking pass
                            'yes'. Defaults to 'no'.
      --host HOST           Cloudera Manager Host
      --port PORT           Cloudera Manager Port
      --username USERNAME   Cloudera Manager User Name
      --password PASSWORD   Cloudera Manager Password
      --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                            is managed by Cloudera Manager.
      --path PATH           Directory where we can save/load configurations files.
                            Defaults to working directory
                            /home/SOME_USER/
      --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                            Log level [INFO|DEBUG|WARNING|FATAL|ERROR]

.. _help_cluster-push:

Help |EM| cluster-push
======================

Combines cluster-generate and cluster-config

.. code::

    $ cluster-push --help
    usage: cluster-push [-h] [--formula FORMULA] [--formula-args FORMULA_ARGS]
                        --update-cdh {no,yes} --restart-cdh {no,yes}
                        [--conflict-merge {interactive,user,generated}] --host
                        HOST [--port PORT] [--username USERNAME]
                        [--password PASSWORD] [--cluster CLUSTER] [--path PATH]
                        [--log {INFO,DEBUG,WARNING,FATAL,ERROR}]

    Auto generate various CDH configurations based on system resources

    optional arguments:
      -h, --help            show this help message and exit
      --formula FORMULA     Auto generation formula file. Defaults to
                            /home/rodorad/Documents/config/atk-
                            config/cluster_config/formula.py
      --formula-args FORMULA_ARGS
                            Auto generation formula arguments to possibly override
                            global configurations.
      --update-cdh {no,yes}
                            Should we update CDH with all configurations in
                            cdh.json/user-cdh.json?
      --restart-cdh {no,yes}
                            Should we restart CDH services after configuration
                            changes
      --conflict-merge {interactive,user,generated}
                            When encountering merge conflicts between the
                            generated configuration() and the user configuration()
                            what value should we default to? The 'user',
                            'generated', or 'interactive'resolution
      --host HOST           Cloudera Manager Host
      --port PORT           Cloudera Manager Port
      --username USERNAME   Cloudera Manager User Name
      --password PASSWORD   Cloudera Manager Password
      --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                            is managed by Cloudera Manager.
      --path PATH           Directory where we can save/load configurations files.
                            Defaults to working directory
                            /home/rodorad/Documents/config/atk-config
      --log {INFO,DEBUG,WARNING,FATAL,ERROR}
                            Log level [INFO|DEBUG|WARNING|FATAL|ERROR]

.. _cluster_generate:

-------------------------------
Generating |CDH| Configurations
-------------------------------

Script: cluster-generate
========================

Description: used for generating optimized |CDH| configurations

Unique Options
--------------

*   **--formula**: path to formula file if not using the default packaged formula.
*   **--formula-args**: path to formula arguments to possibly override constants.

Common Options
--------------

If you forget the command line options run the script with **--help** or
visit the :ref:`help section <help_cluster-generate>`.

Proving only |CDH| connection details should be sufficient for generating
optimized configurations since **--formula** is provided with a default.

The default formula can be found in the repository at
*cluster_cofig/formula.py*.

Example
-------

.. code::

    $ cluster-generate --host CLOUDERA_HOST --port CLODERA_PORT --username CLOUDERA_USER
    What is the Cloudera manager password? 
    --INFO cluster selected: SOME_CLUSTER
    --INFO using formula: cluster_config/formula.py
    --INFO Wrote CDH configuration file to: /home/some-user/cdh.json
    --INFO Wrote ATK generated configuration file to: /home/some-user/generated.conf

After providing the Cloudera manager password, all the cluster details will
be extracted and provided to the formula to calculate an optimized
configuration which gets saved to *cdh.json*.

The generated.conf file will contain ATK specific configurations.
It can be ignored, if you don't intend to use the ATK server for analytics.

The optimized configurations are saved to a file to allow users to view and
verify the configurations before they get saved to |CDH| with the
cluster-config_ script.

Although we take great care to make sure the default formula will work for a
majority of use cases, you should verify the settings to make sure your
cluster will not be adversely affected.

Next we will read *cdh.json* and update |CDH|.

.. _cdh.json:

cdh.json
--------

Here is a chd.json file generated on a 4 node cluster

*   1 node having Cloudera manager
*   3 nodes running as workers nodes
*   32 cores
*   252 gigabytes of memory

.. code::

    {
        "HBASE": {
            "REGIONSERVER": {
                "REGIONSERVER_BASE": {
                    "HBASE_REGIONSERVER_JAVA_HEAPSIZE": 26430567975
                }
            }
        }, 
        "YARN": {
            "GATEWAY": {
                "GATEWAY_BASE": {
                    "MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP": 4129776246, 
                    "MAPREDUCE_MAP_MEMORY_MB": 5368709120, 
                    "MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP": 8259552492, 
                    "MAPREDUCE_REDUCE_MEMORY_MB": 10737418240, 
                    "YARN_APP_MAPREDUCE_AM_MAX_HEAP": 4129776246, 
                    "YARN_APP_MAPREDUCE_AM_RESOURCE_CPU_VCORES": 1, 
                    "YARN_APP_MAPREDUCE_AM_RESOURCE_MB": 5368709120
                }
            }, 
            "NODEMANAGER": {
                "NODEMANAGER_BASE": {
                    "YARN_NODEMANAGER_RESOURCE_CPU_VCORES": 32, 
                    "YARN_NODEMANAGER_RESOURCE_MEMORY_MB": 180388626432
                }
            }, 
            "RESOURCEMANAGER": {
                "RESOURCEMANAGER_BASE": {
                    "YARN_SCHEDULER_INCREMENT_ALLOCATION_MB": 536870912, 
                    "YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB": 180388626432, 
                    "YARN_SCHEDULER_MAXIMUM_ALLOCATION_VCORES": 32, 
                    "YARN_SCHEDULER_MINIMUM_ALLOCATION_MB": 5368709120
                }
            }
        }
    }

Formulas
========

It is possible to create your optimization formulas or copy the default
formula and make your own tweaks.
The formula files are regular python scripts. While the default formula is
called 'formula.py' their are no restrictions.

The formula files are executed with `python's exec file function
<https://docs.python.org/2/library/functions.html#execfile>`__.

Cluster
-------

The cluster variable will contain all installed service, role, config group
and configuration details from the |CDH| cluster we are currently connected to.

Lets look at a code sample from formula.py to see how the cluster variable is
being used.

.. code::

    ...
    hosts = cluster.yarn.nodemanager.hosts.all()
    max_cores = 0
    max_memory = 0
    for key in hosts:
        if hosts[key].numCores > max_cores:
            max_cores = hosts[key].numCores
        if hosts[key].totalPhysMemBytes > max_memory:
            max_memory = hosts[key].totalPhysMemBytes

    ...

On the first line with **cluster.yarn.nodemanager.hosts.all()** we are
retreiving all the details for every host running the yarn node manager role.
The returned object will be a list of cloudera
`apihosts <http://cloudera.github.io/cm_api/apidocs/v10/ns0_apiHost.html>`__.

With the same notation you can retrive the all the details for hosts running
the hbase region server role or all zookeeper servers.

*   **cluster.hbase.regionserver.hosts.all()**
*   **cluster.zookeeper.server.hosts.all()**

You can access all |CDH| services, roles, configs, and hosts with the same notation.

The pattern you want to follow is

*   **cluster.SERVICE.ROLE.CONFIG_GROUP.CONFIG**
*   **cluster.SERVICE.ROLE.hosts.all**

*   **SERVICE** is any valid |CDH| `service
    <http://cloudera.github.io/cm_api/apidocs/v10/path__clusters_-clusterName-_services.html>`__

*   **ROLE** is any valid |CDH| `role
    <http://cloudera.github.io/cm_api/apidocs/v10/path__clusters_-clusterName-_services_-serviceName-_roles.html>`__

*   **CONFIG_GROUP** is any valid config group from cluster-explore_ script.
    A good example of a configuration group would be 'gateway_base' because
    just about every service has this role.

*   **CONFIG** is any valid config from cluster-explore_ script.
    When accessing |CDH| services all attributes are lowercase while the keys
    displayed by the cluster-explore_ script are uppercase.

Accessing Individual Configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Accessing individual configurations is easy but you must know the
configuration name.
You will need to run the cluster-explore_ script to find the full
configuration name.

Here is a config key that was found with cluster-explore_.

.. code::

    - name: HBASE_REGIONSERVER_JAVA_HEAPSIZE
    - description: Maximum size in bytes for the Java Process heap memory.  Passed to Java -Xmx.
    - key: HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE
    - value: 26430567975

To retrive the following value with the cluster variable make the key all
lower case and prefix cluster.

**cluster.hbase.regionserver.regionserver_base.hbase_regionserver_java_heapsize.value**

If you look at the implementation for the 'cluster' objects class you will
notice that you can set all |CDH| configurations directly i would cuation
against it. Saving the computed configurations to the **'cdh'** dictionary will
allow the user to review the configurations when they get saved to *cdh.json*
and enable the saving of snapshot for admnistrative purposes.

Log
---

The log variable has four methods available for sending log messages to the
command line:

*   log.info
*   log.error
*   log.warning
*   log.fatal: does a sys.exit(1) after logging it's message.
    All the methods take a single string as an argument.

Args
----

Args is a dictionay of values provided by the user.
The args dictionary gives us the ability to consider the users Constants when
calculating configurations.

When calculating optimized configurations you often need constants to set
threshholds.
The default **formula.py** has several constants to allow for fine tuning.

..code::

    NUM_THREADS = 1  # This should be set to the maximum number of munti-tanent users
    OVER_COMMIT_FACTOR = 1.30
    MAX_JVM_MEMORY = 32768 * MiB
    MEM_FRACTION_FOR_HBASE = 0.20
    MEM_FRACTION_FOR_OTHER_SERVICES = 0.20
    MAPREDUCE_JOB_COUNTERS_MAX = 500

If the value for **'NUM_THREADS'** is given on the command line, that value
in received in **'args["NUM_THREADS"]'** allowing for the decision of whether
to use the default or the specified value.

CDH
---

The **'cdh'** dictionary is the variable where the calculated |CDH|
configurations are saved with the dictionary key being the full key to the
path found with the cluster-explore_ script.

When the script has finished running, the contents of the **'cdh'** dictionary
are saved to *cdh.json* so the configurations can be reviewed before updating
|CDH|.

For example, if you wanted to save your optimized value for the hbase heap you
would do this::

    cdh['HBASE.REGIONSERVER.REGIONSERVER_BASE.HBASE_REGIONSERVER_JAVA_HEAPSIZE'] = SOME_MEMORY_SETTING

Once the execfile has finished running, the **'cdh'** dictionary is exported to
*cdh.json* which is read by the cluster-config_ or help_cluster-push scripts to
update |CDH|.

ATK
---

Much like the **'cdh'** dictionary, the **'atk'** dictionary is the place where
the calculated |ATK| configurations are saved.
If the |ATK|, is not being used, the dictionary can be safely ignored.

The key for every value in the **'atk'** dictionary would need to be a valid
`atk configuration key
<https://github.com/intel-data/atk/blob/master/conf/examples/application.conf.tpl>`__.

Tips
====

When saving optimized configurations be aware of the expected format.
For example, a memory setting for *yarn*, *hbase*, and *spark* may require
different formats.
*Yarn* may expect configuration values expressed in mega bytes, while
*hbase* and *spark* expect values expressed in bytes.

Unfortunetly, the |CDH| REST API does not always throw formatting errors.
If there are formatting issues, login to |CDH| console and look for
configuration warnings concerning the affected services.

.. _cluster-config:

----------------------------
Setting |CDH| Configurations
----------------------------

Script: **cluster-config**

Description: Used for updating |CDH| with the optimized configurations

Unique Options:

*   **--update-cdh**: Whether to update |CDH| configurations.
    Either yes or no.
*   **--restart-cdh**: Whether to restart |CDH| after updating its configuration.
    Either yes or no.
*   **--conflict-merge**: Conflict resolution preference when encountering
    key conflicts between cdh.json and *user-cdh.json*.
    Defaults to *user-cdh.json*.
    Valid values [user, generated, interactive]

Common Options
==============

If you forget the command line options run the script with **--help** or
visit the :ref:`help section <help_cluster-config>`.

.. warning::

    The **update-cdh**, and **restart-cdh** options are provided as fail-safe
    to keep unwanted changes from potentially breaking a working cluster, and
    to keep restarts from stopping long running jobs.

Example
-------

.. code::

    $ cluster-config --host CLOUDERA_HOST --port CLODERA_PORT --username CLOUDERA_USER --update-cdh yes --restart-cdh yes

    What is the Cloudera manager password?
    --INFO cluster selected: SOME_CLUSTER
    --INFO Reading CDH config file: /home/some-user/cdh.json
    --WARNING Couldn't open json file: /home/rodorad/user-cdh.json
    --INFO Writting merged CDH config file: /home/some-user/merged-cdh.json
    --INFO Updating config group: REGIONSERVER_BASE
    --INFO Updated 1 configuration/s.
    Restarting service : "HBASE"
    .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .

    Deploying configuration for all HBASE roles
    .

    --INFO Updating config group: NODEMANAGER_BASE
    --INFO Updated 2 configuration/s.
    --INFO Updating config group: GATEWAY_BASE
    --INFO Updated 7 configuration/s.
    --INFO Updating config group: RESOURCEMANAGER_BASE
    --INFO Updated 4 configuration/s.
    Restarting service : "YARN"
    .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .

    Deploying configuration for all YARN roles
    .

After connecting to Cloudera Manager, *cdh.json* (generated in the
:ref:`previous step <cluster_generate>`
and *user-cdh.json* (if available) are read.

The file *user-cdh.json* has any user overrides and/or any additional |CDH|
configurations needing to be set.
For this example, the *user-cdh.json* file was omitted.

When **--update-cdh**, and **--restart-cdh** are set to 'yes', the |CDH| needs
to update, and restart |CDH| with the configurations from *cdh.json*/user-cdh.json.

user-cdh.json
=============

The *user-cdh.json* file is where to set any |CDH| configurations.
As long the file is in the working directory or in directory specified by
**--path**, it will be read and pushed to |CDH| by the cluster-config_ script.

A sample *user-cdh.json* file looks like a *cdh.json* file.
It can have the same or different keys as a *cdh.json* file.

Finding Keys
------------

For a simple demonstration, look for any key with the cluster-explore_
script to add to our *user-cdh.json* file.
This example shows
**YARN.JOBHISTORY.JOBHISTORY_BASE.YARN_JOBHISTORY_BIND_WILDCARD**.

.. code::

    config:
    - Name: YARN_JOBHISTORY_BIND_WILDCARD
    - Description: If enabled, the JobHistory Server binds to the wildcard address ("0.0.0.0") on all of its ports.
    - Key: YARN.JOBHISTORY.JOBHISTORY_BASE.YARN_JOBHISTORY_BIND_WILDCARD
    - Value: false

This is what it looks like in |CDH|:

.. image:: /bind_false.*

Converting Keys
---------------

To save the key to user-cdh.json format, the key needs to be split at the
each period with each element being nested inside the previous element.

key: **YARN.JOBHISTORY.JOBHISTORY_BASE.YARN_JOBHISTORY_BIND_WILDCARD** would
convert to::

    {
        "YARN": {
            "JOBHISTORY": {
                "JOBHISTORY_BASE": {
                    "YARN_JOBHISTORY_BIND_WILDCARD": "true"
                }
            }
        }
    }

Lets go ahead and add another key that will conflict with our *cdh.json* file
to see how conflict resolution works.

I added '**YARN_NODEMANAGER_RESOURCE_CPU_VCORES**' which is set to 32 cores
in my *cdh.json*.
Add the key and subtract a couple of cores from the value::

    {
        "YARN": {
            "JOBHISTORY": {
                "JOBHISTORY_BASE": {
                    "YARN_JOBHISTORY_BIND_WILDCARD": "true"
                }
            },
            "NODEMANAGER": {
                "NODEMANAGER_BASE": {
                    "YARN_NODEMANAGER_RESOURCE_CPU_VCORES": 30
                }
            }
        }
    }

Once you have your *user-cdh.json* file make sure you save it to your working directory.

Verifying JSON
--------------

All *user-cdh.json* files need to contain only valid json text.
If you get parsing errors or have any doubts about your formatting, you can
use a `json lint tool <http://jsonlint.com/>`__ to format and error check
your json text.

Create cdh.json
---------------

Once you've saved *user-cdh.json*, run :ref:`cluster_generate` to create a
*cdh.json* file, or verify you have *_cdh.json_* file in the same working
directory where you saved *_user-cdh.json_* file.

Push user-cdh.json
------------------

Running the cluster-config_ script will automatically push *user-cdh.json*
configurations to |CDH| after merging with *cdh.json*.

You should see log messages notifying you that *user-cdh.json* was loaded.

.. code::

    $ cluster-config --host SOME_USER --update-cdh yes --restart-cdh no
    What is the Cloudera manager password?
    --INFO cluster selected: SOME_CLUSTER
    --INFO Reading CDH config file: /home/SOME_USER/cdh.json
    --INFO Reading user CDH config file: /home/SOME_USER/user-cdh.json
    --INFO conflict resolution: first

    Key merge conflict: YARN.NODEMANAGER.NODEMANAGER_BASE.YARN_NODEMANAGER_RESOURCE_CPU_VCORES
    [user]User value: 30
    [auto]Auto generated value: 32
    Optionally you can accept [au] all user values or [ag] all auto generated values.
    --INFO Auto resolving conflicts. Defaulting to user value
    --INFO Writting merged CDH config file: /home/rodorad/merged-cdh.json
    --INFO Updating config group: REGIONSERVER_BASE
    --INFO Updated 1 configuration/s.
    --INFO Updating config group: NODEMANAGER_BASE
    --INFO Updated 2 configuration/s.
    --INFO Updating config group: JOBHISTORY_BASE
    --INFO Updated 1 configuration/s.
    --INFO Updating config group: RESOURCEMANAGER_BASE
    --INFO Updated 4 configuration/s.
    --INFO Updating config group: GATEWAY_BASE
    --INFO Updated 7 configuration/s.

In above example we see notifications when we load *user-cdh.json* and how
conflicts were resolved.

The default behavior when encountering conflicting keys is to resolve them to
the user value.
You can change the default behavior on the command line with the
**--conflict-merge** option.

We also see that a *merged-cdh.json* file was created.
This file gets created when the *user-cdh.json* and *cdh.json* files are
merged.
It is a record of all the configurations that got pushed to |CDH| after
resolving key conflicts.

After pushing our changes our |CDH| value is updated.

.. image:: /bind_true.*

merged-cdh.json
---------------

sample merged-cdh.json file

.. code::

    {
        "HBASE": {
            "REGIONSERVER": {
                "REGIONSERVER_BASE": {
                    "HBASE_REGIONSERVER_JAVA_HEAPSIZE": 26430567975
                }
            }
        },
        "YARN": {
            "GATEWAY": {
                "GATEWAY_BASE": {
                    "MAPREDUCE_MAP_JAVA_OPTS_MAX_HEAP": 4129776246,
                    "MAPREDUCE_MAP_MEMORY_MB": 5368709120,
                    "MAPREDUCE_REDUCE_JAVA_OPTS_MAX_HEAP": 8259552492,
                    "MAPREDUCE_REDUCE_MEMORY_MB": 10737418240,
                    "YARN_APP_MAPREDUCE_AM_MAX_HEAP": 4129776246,
                    "YARN_APP_MAPREDUCE_AM_RESOURCE_CPU_VCORES": 1,
                    "YARN_APP_MAPREDUCE_AM_RESOURCE_MB": 5368709120
                }
            },
            "JOBHISTORY": {
                "JOBHISTORY_BASE": {
                    "YARN_JOBHISTORY_BIND_WILDCARD": "True"
                }
            },
            "NODEMANAGER": {
                "NODEMANAGER_BASE": {
                    "YARN_NODEMANAGER_RESOURCE_CPU_VCORES": 30,
                    "YARN_NODEMANAGER_RESOURCE_MEMORY_MB": 180388626432
                }
            },
            "RESOURCEMANAGER": {
                "RESOURCEMANAGER_BASE": {
                    "YARN_SCHEDULER_INCREMENT_ALLOCATION_MB": 536870912,
                    "YARN_SCHEDULER_MAXIMUM_ALLOCATION_MB": 180388626432,
                    "YARN_SCHEDULER_MAXIMUM_ALLOCATION_VCORES": 32,
                    "YARN_SCHEDULER_MINIMUM_ALLOCATION_MB": 5368709120
                }
            }
        }
    }

.. _cluster-explore:

----------------------------
Exploring CDH Configurations
----------------------------

Script: **cluster-explore**

Description: Browse |CDH| configurations by service, role, and configuration
group or dumped all at once to the screen.
Helps find the configuration keys if you want to write your own
*user-cdh.json* file or if you want to write your own formula file.

Unique Options:

*   **--dump**: if 'yes' dumps all the configurations for every service to
    stdout.

Common Options
==============

If you forget the command line options run the script with **--help** or
visit the :ref:`help section <help_cluster-explore>`.

Finding configurations is quite difficult without an appropriate tool.
The cluster explore script solves this very problem.

It has two run modes:

1.  Print all the configurations for every service to the screen.
#.  Interactively print all the configurations for a specific service, role,
    and configuration group.

To invoke option 1 you can answer 'yes' to the --dump command line option or
answer yes when prompted on the command line.

.. code::

    $ cluster-explore --host SOME_HOST --port SOME_PORT --username SOME_USERNAME
    What is the Cloudera manager password? 
    --INFO cluster selected: SOME_CLUSTER
    dump all configs[yes or no]: yes

    ...
    - config name: ENABLE_CONFIG_ALERTS 
    - config description: When set, Cloudera Manager will send alerts when this entity's configuration changes.
    - config key: SPARK_ON_YARN.SPARK_YARN_HISTORY_SERVER.SERVER_BASE.ENABLE_CONFIG_ALERTS
    - config value: false

    - config name: PROCESS_AUTO_RESTART 
    - config description: When set, this role's process is automatically (and transparently) restarted in the event of an unexpected failure.
    - config key: SPARK_ON_YARN.SPARK_YARN_HISTORY_SERVER.SERVER_BASE.PROCESS_AUTO_RESTART
    - config value: false
    ...

To invoke option 2, run the script interactively.
Answer "no" or hit enter when prompted to dump the configurations.

.. code::

    $ cluster-explore --host SOME_HOST --port SOME_PORT --username SOME_USERNAME
    What is the Cloudera manager password?
    --INFO cluster selected: SOME_CLUSTER
    dump all configs[yes or no]: no

    Available service types on cluster: 'cluster'
    Pick a service
    Id 1 service: HDFS
    Id 2 service: HBASE
    Id 3 service: ZOOKEEPER
    Id 4 service: YARN
    Id 5 service: SPARK_ON_YARN
    Enter service Id : 4
    Selected YARN

    Available role types on service: 'YARN'
    Pick a role
    Id 1 role: NODEMANAGER
    Id 2 role: JOBHISTORY
    Id 3 role: RESOURCEMANAGER
    Id 4 role: GATEWAY
    Enter role Id : 4
    Selected GATEWAY

    Available config group types on role: 'GATEWAY'
    Pick a config group
    Id 1 config group: GATEWAY_BASE
    Enter config group Id : 1
    Selected GATEWAY_BASE

    ...

    config:
    - name: MAPRED_OUTPUT_COMPRESSION_CODEC
    - description: For MapReduce job outputs that are compressed, specify the compression codec to use. Will be part of generated client configuration.
    - key: YARN.GATEWAY.GATEWAY_BASE.MAPRED_OUTPUT_COMPRESSION_CODEC
    - value: org.apache.hadoop.io.compress.DefaultCodec

    config:
    - name: MAPRED_REDUCE_PARALLEL_COPIES
    - description: The default number of parallel transfers run by reduce during the copy (shuffle) phase. This number should be between sqrt(nodes*number_of_map_slots_per_node) and nodes*number_of_map_slots_per_node/2. Will be part of generated client configuration.
    - key: YARN.GATEWAY.GATEWAY_BASE.MAPRED_REDUCE_PARALLEL_COPIES
    - value: 10

    config:
    - name: MAPREDUCE_JOB_UBERTASK_MAXMAPS
    - description: Threshold for number of maps, beyond which a job is considered too big for ubertask optimization.
    - key: YARN.GATEWAY.GATEWAY_BASE.MAPREDUCE_JOB_UBERTASK_MAXMAPS
    - value: 9

    ...

For every configuration key you will see

*   configuration name: Normalized configuration name to account for some
    weirdness with CDH configuration names like slashes and dashes.
*   configuration description: This will be the description from CDH UI.
*   full configuration key path: The format for a key is
    SERVICE_TYPE.ROLE_TYPE.CONFIG_GROUP_NAME.CONFIGURATION_KEY.
    While composed mostly of types the CONFIG_GROUP_NAME is normalized to
    remove service names which can be anything.
*   value: The value of the configuration.

