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
    host:    localhost
    port:    9099
    scheme:  http
    version: v1

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
To see the full Python stack trace of the last (i.e. most recent) exception:

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

Tab Completion
==============

Allows you to use the tab key to complete your typing for you.

If you are running with a standard Python REPL (not iPython, bPython, or the
like) you will have to set up the tab completion manually:

Create a .pythonrc file in your home directory with the following contents:

.. code::

    >>> import rlcompleter, readline
    >>> readline.parse_and_bind('tab:complete')


Or you can just run the two lines in your REPL session.

This will let you do the tab completion, but will also remember your history
over multiple sessions:

.. code::

    # Add auto-completion and a stored history file of commands to your Python
    # interactive interpreter. Requires Python 2.0+, readline.

    >>> import atexit
    >>> import os
    >>> import readline
    >>> import rlcompleter
    >>> import sys

    # Autocomplete is bound to the Esc key by default, so change it to tab.
    >>> readline.parse_and_bind("tab: complete")

    >>> historyPath = os.path.expanduser("~/.pyhistory")

    >>> def save_history(historyPath=historyPath):
    ...     import readline
    ...     readline.write_history_file(historyPath)

    >>> if os.path.exists(historyPath):
    ...     readline.read_history_file(historyPath)

    >>> atexit.register(save_history)

    # anything not deleted (sys and os) will remain in the interpreter session
    >>> del atexit, readline, rlcompleter, save_history, historyPath

Note:
    If the .pythonrc does not take effect, add PYTHONSTARTUP in your .bashrc
    file:

    .. code::

        export PYTHONSTARTUP=~/.pythonrc

.. Outdated 20150727::

    .. index::
        single: Spark

    -----
    Spark
    -----

    Resolving disk full issue while running Spark jobs
    ==================================================

    Using a Red Hat cluster, or an old CentOS cluster,
    the /tmp drive may become full while running spark jobs.
    This causes the jobs to fail, and it is caused by the way the /tmp file system
    is setup,
    Spark and other |CDH| services, by default, use /tmp as the temporary location
    to store files required during run time, including, but not limited to, shuffle
    data.

    Steps to resolve this issue:

    1)  Stop the Trustedanalytics service.
    #)  From |CDH| Web UI:

        a)  Stop the Cloudera Management Service.
        #)  Stop the |CDH|.

    #)  Now run the following steps on each node:

        a)  Find the largest partition by running the command::

                $ df -h

        #)  Assuming /mnt is your largest partition, create the folder
            "/mnt/.bda/tmp", if it isn't already present::

                $ sudo mkdir -p /mnt/.bda/tmp

        #)  Set the permissions on this directory so that it's wide open::

                $ sudo chmod 1777 /mnt/.bda/tmp

        #)  Add the following line to your '/etc/fstab' file and save it::

                /mnt/.bda/tmp    /tmp    none   bind   0   0

        #)  Reboot the machine.

    Spark space concerns
    ====================
    Whenever you run a Spark application, jars and logs go to '/va/run/spark/work'
    (or other location configured in Cloudera Manager).
    These can use up a bit of space eventually (over 140MB per command).

    * Short-term workaround: periodically delete these files
    * Long-term fix: Spark 1.0 will automatically clean up the files

    ----------
    References
    ----------

    `Spark Docs <https://spark.apache.org/documentation.html>`__

