.. _ad_scoring_engine:

Scoring Engine
==============

This section covers the scoring engine installation, configuration
and running the scoring engine.

Installation
------------

The scoring engine repositories are automatically installed as part of the
ATK repositories.
Please see section ATK packages installation.

Scoring Models Implementation
-----------------------------

The scoring engine is a generic engine and is oblivious to the streaming
scoring model.
It is up to the user of the engine to provide the scoring implementation.
The implementation, model bytes and the class name is provided in a tar file
at the startup of the scoring engine.
The scoring engine expects three files in the tar file as listed below.

1)  The model implementation jar file.
    This is the 
2)  A file by the name modelname.txt that contains the name of the class that
    implements the scoring in the jar file.
3)  The file that will have the model bytes that will be used by the scoring.
    The name of this file will the name of the URL of the rest server.
    See section starting_scoring_engine_ below.

.. note::
   
    If you are using ATK to score, the publish method on the models will
    create the tar file that can used as input to the scoring engine.

Configuration of the Engine
---------------------------

*/etc/trustedanalytics/scoring/application.conf*

The scoring engine provides a configuration template file which must be used
to create a configuration file.
Copy the configuration template file *application.conf.tpl* in the same
directory, like this::

    $ cd /etc/trustedanalytics/scoring
    $ sudp cp application.conf.tpl application.conf

Open the file with a text editor::

    $ sudo vi application.conf

Modify the section below to point to the where the scoring tar file is located.
Below is an example::

    trustedanalytics.scoring-engine {
      archive-tar = "hdfs://scoring-server.intel.com:8020/user/atkuser/kmeans.tar"
    }

.. _starting_scoring_engine:

Starting the Scoring Engine Service
-----------------------------------

Once the application.conf file has been modified to point to the scoring tar
file, the scoring engine can be started with the following command::

    $ sudo service scoring-engine start

This will launch the rest server for the engine.
The REST API is::

    GET /v1/models/[name]?data=[urlencoded record 1]


Scoring Client
--------------

Below is a sample python script to connect to the scoring engine::

    >>> import requests
    >>> import json
    >>> headers = {'Content-type': 'application/json',
    ...            'Accept': 'application/json,text/plain'}
    >>> r = requests.post('http://localhost:9099/v1/models/testjson?data=2', headers=headers)



