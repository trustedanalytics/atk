.. _ds_pycharm:

.. index:: ! PyCharm
    single: IDE

==========================
PyCharm Setup Instructions
==========================

PyCharm is a Python IDE created by JetBrains.

.. contents:: Table of Contents
    :local:
    :backlinks: none

-------------
Prerequisites
-------------

These instructions assume you have already installed:

-   Python 2.7.
-   |PACKAGE| Python REST Client and required dependencies.
-   `PyCharm <http://www.jetbrains.com/pycharm/>`_.

|PACKAGE| should work with any version of PyCharm but these instructions were
tested with PyCharm Community Edition 3.4.1.

-----
Setup
-----

1)  Select *New Project* on PyCharm's initial screen.

    #)  Give your project a name, for example, "myproject".
    #)  Choose *Python 2.7* as the Python Interpreter and choose the *OK*
        button.

        i)  If *Python 2.7* does not appear in the list you will need to
            configure a Python 2.7 interpreter.

            1)  Choose the button that looks like a "gear".
            #)  Choose *Add Local*.
            #)  Browse for your local Python 2.7 installation.
                On RedHat or CentOS this is probably /usr/bin/python.
            #)  Choose the *OK* button.

#)  Choose :menuselection:`File --> Settings`.

    a)  Choose *Project Structure*.
    #)  Choose *Add Content Root* and browse to the |PACKAGE| Python REST Client
        libraries.
        On RedHat or CentOS these are found under
        '/usr/lib/trustedanalytics/rest-client/python'.
    #)  Choose *Apply* button.
    #)  Choose *OK* button.

#)  Right click your project folder, for example, "myproject", and select
    :menuselection:`New --> Python File`.

    #)  In order to connect to the analytics instance in TAP follow instructions :doc:`here </python_api/connect>`
