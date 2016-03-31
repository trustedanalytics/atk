.. _ad_other:

.. _windows_python_setup:

.. index::
    pair: Windows; Python
    single: Anaconda
    single: Python

--------------------
Windows Python Setup
--------------------

If you are behind  a corporate firewall you will need your windows proxy settings before trying to run any commands.
To use the ATK client on a windows machine you will need to install the anaconda python distribution. The installation is done through an msi and can be found on the anocanda download page.
 
Before installing the ATK client you must first install numpy and pandas through anaconda.
You will also need to update pip.
Open a windows command line and run the following command.

------------------------
Install Pandas and Numpy
------------------------

.. code::
    $ conda install pandas
    $ conda update pip

Numpy will be installed automatically since it's a dependency for pandas.

Once pandas and numpy are installed you can install the ATK client through pip with the following command

------------------
Install ATK client
------------------

.. code::
    $ pip install trustedanalytics

All the dependencies for the Intel Analytics package will be installed automatically.
    Weekly and production releases of client are available on pypi.python.org. If you need to install a weekly build you must specify the version right after the package name

.. code::
    $ pip install trustedanalytics== 0.4.2.dev201512019643

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


