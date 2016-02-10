.. _ds_eclipse:

.. index:: ! Eclipse
    single: IDE

==========================
Eclipse Setup Instructions
==========================

.. contents:: Table of Contents
    :local:
    :backlinks: none

-------------
Prerequisites
-------------

These instructions assume you have already installed:

-   :ref:`Python 2.7 <windows_python_setup>`.
-   |PACKAGE| :ref:`Python REST Client <installing_ta_packages>` and required
    dependencies.
-   Eclipse Standard.

|PACKAGE| should work with any version of Eclipse, but these instructions were
tested with Eclipse Standard Version 4.4 Luna.

If you are on a corporate network, you may need to configure proxy settings in
Eclipse before beginning (see `Eclipse Help
<http://help.eclipse.org/luna/index.jsp?topic=%2Forg.eclipse.jpt.doc.user%2Ftips_and_tricks.htm>`__ ).

-----
Setup
-----

1)  Download, install, and start the
    `Eclipse IDE <http://www.eclipse.org/>`__.
#)  Choose :menuselection:`Help --> Eclipse Marketplace`.

    .. image:: ds_eclipse_01.*
        :width: 40%
        :align: center

    If the marketplace screen does not come up, you may need to configure
    proxy settings (see `Eclipse Help
    <http://help.eclipse.org/luna/index.jsp?topic=%2Forg.eclipse.jpt.doc.user%2Ftips_and_tricks.htm>`__ ).
#)  Search for "PyDev" and choose *PyDev - Python IDE for Eclipse 3.6.0* or
    newer version.

    a)  Choose *Confirm* button.
    #)  Choose *Accept* when prompted for license agreement.
    #)  If prompted "Do you trust these certificates?".
        Select "Brainwy Software; PyDev; Brainwy" and choose the *OK* button.
    #)  When prompted to restart Eclipse, choose the *Yes* button.

#)  Choose the default Workspace.
#)  Choose :menuselection:`File --> New --> Project...`.

    a)  Choose the *PyDev* folder and *PyDev Project* and choose the *Next*
        button.
    #)  Give your project a name, for example "myproject".
    #)  Choose version 2.7.
    #)  Choose *Please configure an interpreter before proceeding*.

        i)  Choose *Manual Configure*.
        #)  Choose the *New* button.
        #)  Browse for Python 2.7.  On RedHat and CentOS this is probably
            /usr/bin/python.
        #)  Choose the *Ok* button.
        #)  Choose the *Ok* button.

    #)  Select the interpreter you just setup from the Interpreter drop-down.
    #)  Choose the *Finish* button.
    #)  When prompted "This kind of project is associated with the PyDev
        perspective. Do you want to open this perspective now?" choose *Yes*.

#)  Right click your project folder, for example, "myproject".

    a)  Choose *Properties*.
    #)  Choose *PyDev - PYTHONPATH* in the left hand pane.
    #)  Choose the *External Libraries* tab.
    #)  Choose *Add source folder* button.
    #)  Browse for the |PACKAGE| Python REST Client libraries.
        On RedHat and CentOS these are found under
        '/usr/lib/trustedanalytics/rest-client/python'.
    #)  Choose the *OK* button.

#)  Right click your project folder, for example, "myproject".

    a)  Choose :menuselection:`New --> Source Folder`.
    #)  Give it the name "src" and choose the *Finish* button.

#)  Right click *src* folder and choose :menuselection:`New --> File`.

    a)  Give the file name 'test.py'.
    #)  If prompted, confirm the default settings for PyDev by choosing *OK*.
    #)  Close the *Help keeping PyDev alive* dialog, if it appears.
    #)  In order to connect to the analytics instance in TAP follow instructions :doc:`here </python_api/connect>`

#)  Next take a look at the included examples.

