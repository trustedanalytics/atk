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

Tab Completion
==============

Allows you to use the tab key to complete your typing for you.

If you are running with a standard Python REPL (not IPython, bPython, or the
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

    # Auto-complete is bound to the ESC key by default, so change it to tab.
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

