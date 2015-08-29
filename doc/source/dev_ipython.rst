.. index:: ! IPython

==========================
IPython Setup Instructions
==========================

.. contents:: Table Of Contents
    :local:
    :backlinks: none

------------
Introduction
------------

These instructions show how to use the |PACKAGE| server through
`IPython <http://ipython.org/>`__.
This is a guide through the IPython setup needed to communicate with an |PACKAGE|
service on a remote cluster.
With an understanding of this information, the Python REST client can be
accessed from a remote host through an IPyhon shell or notebook server.

------------
Requirements
------------

A working |PACKAGE| cluster installation is required.
It must be configured to run with the Python2.7 executable.
If the |PACKAGE| is not installed, see :doc:`/ad_inst_ta1` to install it.

Installing IPython
==================

To install IPython run::

    $ sudo yum install python27-ipython

----------------------------------
Configure |PACKAGE| Python REST Client
----------------------------------

Before IPython can operate properly, it is necessary to configure the |PACKAGE|
REST client.
The REST client needs to know where to find the |PACKAGE| REST server.
This is done by updating the host address in
'/usr/lib/trustedanalytics/rest-client/python/rest/config.py'::

    $ sudo vim /usr/lib/trustedanalytics/rest-client/python/rest/config.py

The 'config.py' file will look similar to this::

    """
    config file for REST client
    """
    # default connection config
    class server:
        host = "localhost"
        port = 9099
        scheme = "http"
        version = "v1"
        headers = {'Content-type': 'application/json',
                'Accept': 'application/json,text/plain',
                'Authorization': "test_api_key_1"}

    class polling:
        start_interval_secs = 1
        max_interval_secs = 20
        backoff_factor = 1.02

    build_id = None

Update the address for host to the `Fully Qualified Domain Name
<http://en.wikipedia.org/wiki/Fully_qualified_domain_name>`_ or
the IP address of the node hosting the |PACKAGE| REST server.

---------------
Running IPython
---------------

Test the |PACKAGE| IPython installation by importing the REST client libraries
inside of a notebook or IPython shell and ping the REST server.
::

    # testing IPython/Trusted Analytics ATK

    $ ipython
    Python 2.7.5 (default, Sep  4 2014, 17:06:50)
    Type "copyright", "credits" or "license" for more information.
    IPython 2.2.0 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: import trustedanalytics as atk

    In [2]: atk.server.ping()
    Successful ping to Trusted Analytics ATK at http://localhost:9099/info

    In [3]: atk.connect()

IPython Notebook
================

All the dependencies to run the IPython notebook server are also installed
which lets the IPython shell be run from a web browser.
The notebook server is accessed by::

    $ ipython notebook

