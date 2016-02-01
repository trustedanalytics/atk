.. currentmodule:: trustedanalytics

.. index::
       single: connect

Connect to the Server
=====================

.. contents:: Table of Contents
    :local:
    :backlinks: none

The Python client must 'connect' to an |PACKAGE| server before it can be used.
Here is the 'connect' process described by the method's documentation:

.. autofunction:: trustedanalytics.connect

Basic connecting
----------------

To use the default settings provided by the environment and/or configuration:

.. code::

    >>> import trustedanalytics as ta
    >>> ta.connect()

To connect to a specific server:

.. code::

    >>> import trustedanalytics as ta
    >>> ta.server.uri = 'myhost-name:port'
    >>> ta.connect()

Connections requiring OAuth
---------------------------

To connect to a TAP instance of |PACKAGE|, the python client must have an OAuth access token (see [oauth tokens](http://self-issued.info/docs/draft-ietf-oauth-v2-bearer.html)).  
The user must have a credentials file which holds an OAuth access token and a refresh token.

The user can create a credentials file using |PACKAGE| client running in an interactive python session.  Call `create_credentials_file('filename_of_your_choice')` and interactively provide answers to its prompt.

.. PaaS::

    To connect to an Analytics PaaS instance of |PACKAGE|, the python client must have an OAuth access token (see `oauth tokens <http://self-issued.info/docs/draft-ietf-oauth-v2-bearer.html>`).
    The user must have a credentials file which holds an OAuth access token and a refresh token.

    The user can create a credentials file using |PACKAGE| client running in an interactive python session.
    Call ``create_connect_file('filename_of_your_choice')`` and interactively provide answers to its prompt.

.. code::

    $ python2.7

    >>> import trustedanalytics as ta
    >>> ta.create_connect_file('~/.ta/demo.creds')
    OAuth server URI: uaa.my-tap-domain.com
    user name: dscientist9
    Password: **********

    Credentials created at '/home/dscientist9/.ta/demo.creds'

The credentials file can be specified when calling ``connect`` or set as an environmental variable $TA_CREDS.

.. only:: html

    .. code::

        >>> ta.connect('~/.ta/demo.creds')
        Connected.  This client instance connected to server http://my-ta-instance.my-tap-apps-domain.com/v1 as user dscientist9 at 2015-06-19 10:27:21.583704.

.. only:: latex

    .. code::

        >>> ta.connect('~/.ta/demo.creds')
        Connected.  This client instance connected to
        server http://my-ta-instance.my-tap-apps-domain.com/v1
        as user dscientist9 at 2015-06-19 10:27:21.583704.

The credentials file path must be relative to how python was launched.
Full paths are recommended.
Multiple credentials files can be created.
They should be protected with appropriate OS privileges.

Using Environmental Variables
-----------------------------

The URI of the |PACKAGE| server can be specified by the environmental variable ``$TA_URI``.
The python client will initialize its config setting to this value.
It may still be overridden as shown above in the session or script.

.. code::

    $ export TA_URI=ta-server.demo-gotapaas.com

The credentials file can be specified by $TA_CREDS.

.. code::

    $ export TA_CREDS=~/.ta/demo.creds

With these two variables set, the simple connect sequence works.

.. code::

    >>> import trustedanalytics as ta
    >>> ta.connect()

Troubleshooting
---------------

Client's Server Settings
~~~~~~~~~~~~~~~~~~~~~~~~

To see the client's configuration to find the server, look at ``ta.server``:

.. only:: html

    .. code::

        >>> ta.server
        {
            "headers": {
                "Accept": "application/json,text/plain", 
                "Authorization": "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIyOTllYmMxZC0zNDgyLTRhOWEtODM2ZC03ZDM1ZmIzZWZiNmYiLCJzdWIiOiJiZTYzMWQ1OS1iYWM4LTRiOWQtOTFhNy05NzMyMTBhMWRhMTkiLCJzY29wZSI6WyJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMucmVhZCIsImNsb3VkX2NvbnRyb2xsZXIud3JpdGUiLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyLnJlYWQiXSwiY2xpZW50X2lkIjoiYXRrLWNsaWVudCIsImNpZCI6ImF0ay1jbGllbnQiLCJhenAiOiJhdGstY2xpZW50IiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6ImJlNjMxZDU5LWJhYzgtNGI5ZC05MWE3LTk3MzIxMGExZGExOSIsInVzZXJfbmFtZSI6ImFuamFsaS5zb29kQGludGVsLmNvbSIsImVtYWlsIjoiYW5qYWxpLnNvb2RAaW50ZWwuY29tIiwiaWF0IjoxNDM0NzUyODU4LCJleHAiOjE0MzQ3OTYwNTgsImlzcyI6Imh0dHBzOi8vdWFhLmRlbW8tZ290YXBhYXMuY29tL29hdXRoL3Rva2VuIiwiYXVkIjpbImF0ay1jbGllbnQiLCJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMiLCJjbG91ZF9jb250cm9sbGVyIiwib3BlbmlkIl19.PAwF2OtC0Wd97-gmZ4OXQ36xpyaeCCUC2ErGgCk619m7s6uCGcqydrWveTtgehEjIkZxZ5jfaFI53_bU0cHLseKlxMi1llggk6xC0rWnaUePF47pw-u6eGm2z-rPIqP9i4_2TdTxDKCe9_qziNTQzKOlrn2_yN6KSgtytGEKxkE", 
                "Content-type": "application/json"
            }, 
            "scheme": "http", 
            "oauth_uri": "uaa.my-tap-domain.comdemo-gotapaas.com", 
            "user": "dscientist9"
        }

.. only:: latex

    .. code::

        >>> ta.server
        {
            "headers": {
                "Accept": "application/json,text/plain", 
                "Authorization": "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIyOTllYmMxZC0zNDgyLTRhOWEtODM2Z
                C03ZDM1ZmIzZWZiNmYiLCJzdWIiOiJiZTYzMWQ1OS1iYWM4LTRiOWQtOTFhNy05NzMyMTBhMWRhMTkiL
                CJzY29wZSI6WyJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMucmVhZCIsImNsb3VkX
                2NvbnRyb2xsZXIud3JpdGUiLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyLnJlYWQiXSwiY2xpZW50X
                2lkIjoiYXRrLWNsaWVudCIsImNpZCI6ImF0ay1jbGllbnQiLCJhenAiOiJhdGstY2xpZW50IiwiZ3Jhb
                nRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6ImJlNjMxZDU5LWJhYzgtNGI5ZC05MWE3LTk3MzIxM
                GExZGExOSIsInVzZXJfbmFtZSI6ImFuamFsaS5zb29kQGludGVsLmNvbSIsImVtYWlsIjoiYW5qYWxpL
                nNvb2RAaW50ZWwuY29tIiwiaWF0IjoxNDM0NzUyODU4LCJleHAiOjE0MzQ3OTYwNTgsImlzcyI6Imh0d
                HBzOi8vdWFhLmRlbW8tZ290YXBhYXMuY29tL29hdXRoL3Rva2VuIiwiYXVkIjpbImF0ay1jbGllbnQiL
                CJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMiLCJjbG91ZF9jb250cm9sbGVyIiwib
                3BlbmlkIl19.PAwF2OtC0Wd97-gmZ4OXQ36xpyaeCCUC2ErGgCk619m7s6uCGcqydrWveTtgehEjIkZx
                Z5jfaFI53_bU0cHLseKlxMi1llggk6xC0rWnaUePF47pw-u6eGm2z-rPIqP9i4_2TdTxDKCe9_qziNTQ
                zKOlrn2_yN6KSgtytGEKxkE", 
                "Content-type": "application/json"
            }, 
            "scheme": "http", 
            "oauth_uri": "uaa.my-tap-domain.comdemo-gotapaas.com", 
            "user": "dscientist9"
        }

The settings may be individually modified with the ``ta.server`` object, before calling connect.

