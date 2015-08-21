===============
Getting Started
===============

.. contents:: Table of Contents
    :local:
    :backlinks: none

-----------
Open-Source
-----------

The |PACKAGE| uses standards and open-source routines from
`Apache Hadoop <http://hadoop.apache.org/>`__ such as |HDFS|,
:term:`MapReduce`, YARN, as well as
`Apache Giraph <http://giraph.apache.org/>`__ for graph-based machine learning
and graph analytics.
The Titan graph database can be queried using the
`Gremlin <https://github.com/tinkerpop/gremlin/wiki>`__ graph query
language from TinkerPop.

--------
Features
--------

*   Import routines read and convert data from several different formats
*   Data cleaning tools prepare the data by removing erroneous values,
    transforming values to a normalized state and constructing
    new features through manipulating existing values
*   Analysis and machine learning algorithms give deeper insight into the data

---------------
Script Examples
---------------

The |PACKAGE| ships with example Python scripts and data sets that exercise the
various features of the platform.
The default location for the example scripts is |PACKAGE_USER|'s home directory
'/home/|PACKAGE_USER|'.

The examples are located in '/home/trustedanalytics/examples'::

    -rwxr-xr-- 1 atkuser atkuser  904 Jul 30 04:20 als.py
    -rwxr-xr-- 1 atkuser atkuser  921 Jul 30 04:20 cgd.py
    -rwxr-xr-- 1 atkuser atkuser 1078 Jul 30 04:20 lbp.py
    -rwxr-xr-- 1 atkuser atkuser  707 Aug  7 18:21 lda.py
    -rwxr-xr-- 1 atkuser atkuser  930 Jul 30 04:20 lp.py
    -rwxr-xr-- 1 atkuser atkuser  859 Jul 30 04:20 movie_graph_5mb.py
    -rwxr-xr-- 1 atkuser atkuser  861 Jul 30 04:20 movie_graph_small.py
    -rwxr-xr-- 1 atkuser atkuser  563 Jul 30 04:20 pr.py

The datasets are located in '/home/trustedanalytics/examples/datasets' and
'hdfs://user/trustedanalytics/datasets/'::

    -rw-r--r--  ...  /user/trustedanalytics/datasets/README
    -rw-r--r--  ...  /user/trustedanalytics/datasets/apl.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/lbp_edge.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/lp_edge.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/movie_sample_data_5mb.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/movie_sample_data_small.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/recommendation_raw_input.csv
    -rw-r--r--  ...  /user/trustedanalytics/datasets/test_lda.csv

The datasets in '/home/trustedanalytics/examples/datasets' are for reference.
The actual data that is being used by the Python examples and the |PACKAGE| server
is in the |HDFS| system.

To get access to the scripts, login as atkuser and go to the example scripts
directory::

    $ sudo su atkuser
    $ cd /home/trustedanalytics/examples

To run any of the Python example scripts type::

    $ python <SCRIPT_NAME>

where "<SCRIPT_NAME>" is any of the scripts.

