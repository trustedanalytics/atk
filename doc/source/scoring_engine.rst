.. _scoring_engine:

Scoring Engine
==============

This section covers the scoring engine installation, configuration and
start-up.

Installation
------------

The scoring engine repositories are automatically installed as part of the
|ATK| repositories.

Scoring Models Implementation
-----------------------------

The scoring engine is independent of the streaming scoring model implementation.
To obtain information concerning the model implementation, the scoring engine
expects three files in a tar file:

#)  The model implementation jar file.
#)  A file *modelname.txt* that contains the name of the class that
    implements the scoring in the jar file.
#)  The file that has the model bytes used by the scoring.

.. note::

    If |PACKAGE| is used to build models, the *publish* method on the
    model will create the tar file needed by the scoring engine.

Configuration of the Engine
---------------------------

The scoring engine provides a configuration template file
*application.conf.tpl* which is used to create a working configuration file
*application.conf*.
Copy the configuration template file to the working file name in the same
folder:

.. code::

    $ cd /etc/trustedanalytics/scoring
    $ sudo cp application.conf.tpl application.conf

Open the file with a text editor:

.. code::

    $ sudo vim application.conf

Modify the scoring engine section to indicate where the scoring
tar file is located:

.. code::

    trustedanalytics.scoring-engine {
      archive-tar = "hdfs://scoring-server.company.com:8020/user/atkuser/kmeans.tar"
    }

Starting the Scoring Engine Service
-----------------------------------

Once the application.conf file has been modified, the scoring engine can be
started:

.. code::

    $ sudo service scoring-engine start

Launch the rest server for the engine:

.. code::

    GET /v1/models/[name]?data=[urlencoded record 1]

See the :ref:`REST API <rest_api/v1/index>` for more information.

Scoring Client
--------------

An example python script to connect to the scoring engine:

.. code::

    >>> import requests
    >>> import json
    >>> headers = {'Content-type': 'application/json',
    ...            'Accept': 'application/json,text/plain'}
    >>> r = requests.post('http://localhost:9100/v1/models/testjson?data=2', headers=headers)

