.. _ds_bkm:

=========================
Best Known Methods (User)
=========================

.. contents:: Table of Contents
    :local:
    :backlinks: none

.. index::
    single: Python

------
Python
------

Server Connection
=================

Ping the server:

.. code::

    >>> import trustedanalytics as ta
    >>> ta.server.ping()
    Successful ping to Trusted Analytics ATK at http://localhost:9099/info
    >>> ta.connect()

View and edit the server connection:

.. code::

    >>> print ta.server
    ------------------------------------------------------------------------------
    headers        : {u'Content-type': u'application/json', u'Authorization': u...
    scheme         : http
    uri            : 10.54.8.187:9099
    user           : test_api_key_1
    ------------------------------------------------------------------------------

    >>> ta.server.host
    'localhost'

    >>> ta.server.host = '10.54.99.99'
    >>> ta.server.port = None
    >>> print ta.server
    host:    10.54.99.99
    port:    None
    scheme:  http
    version: v1

Reset configuration back to defaults:

.. code::

    >>> ta.server.reset()
    >>> print ta.server
    host:    localhost
    port:    9099
    scheme:  http
    version: v1

Errors
======

By default, the toolkit does not print the full stack trace when exceptions
occur.
To see the full Python stack trace of the last (in other words, the most
recent) exception:

.. code::

    >>> ta.errors.last

To enable always printing the full Python stack trace, set the *show_details*
property:

.. code::

    >>> import trustedanalytics as ta

    # show full stack traces
    >>> ta.errors.show_details = True

    >>> ta.connect()

    # … the rest of your script …

If you enable this setting at the top of your script you get better error
messages.
The longer error messages are really helpful in bug reports, emails about
issues, etc.

