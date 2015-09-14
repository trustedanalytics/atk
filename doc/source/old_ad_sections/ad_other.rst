.. _old_ad_sections/ad_other:

.. _ad_other:

.. _windows_python_setup:

.. index::
    pair: Windows; Python
    single: Anaconda
    single: Python

--------------------
Windows Python Setup
--------------------

1.  Download Anaconda with Python 2.7.

#.  Install

    -   By default, Anaconda installs to the user's AppData hidden folder.
        It's better to put this in a more accessible location, like
        ``c:\anaconda``.
        This is the only change from the default installation necessary.

#.  Open a command prompt.

#.  Run the command:
    ``conda create -n trustedanalytics-python python=2.7 numpy=1.8 requests=2.3
    ordereddict=1.1``.
    This creates a virtual Python environment that mimics the cluster's
    configuration.

.. index::
    single: IDE
    single: Eclipse
    single: IntelliJ
    single: PyCharm

-----------------------------------
Integrated Development Environments
-----------------------------------
.. toctree::

    ds_eclipse
    ds_intellij
    ds_pycharm


