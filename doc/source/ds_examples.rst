.. _ds_examples:

===============
Python examples
===============

.. contents:: Table of Contents
    :local:
    :backlinks: none

------------
Introduction
------------
At this point you have a running ATK server and you have installed and configured the ATK python client and are wondering what you can do with the platform luckily the ATK python client ships with sample datasets and examples. While the examples are by no means comprehensive they do serve as an introduction.


------------
Requirements
------------
  Before we start you will need

* `Configured Trusted Analytics ATK rest server <https://github.com/trustedanalytics/platform-wiki/wiki/ATK-deployment-procedure-using-app-launching-service-broker-and-buildpack>`__
* `Configured Trusted Analytics ATK python client <https://github.com/trustedanalytics/atk/wiki/python-client>`__
* `Access to the Trusted Analytics data catalog <https://github.com/trustedanalytics/platform-wiki/wiki/Using-Data-Catalog-to-download-files-from-S3>`__

--------
Datasets
--------

For convenience all the datasets used in the examples ship with the client and are also available publicly in `aws <https://analytics-tool-kit.s3-us-west-2.amazonaws.com/index.html?prefix=public/production/v0.4.0/datasets/>`__.

* `movie_data_random.csv <https://analytics-tool-kit.s3-us-west-2.amazonaws.com/public/production/v0.4.0/datasets/movie_data_random.csv>`__
* `cities.csv <https://analytics-tool-kit.s3-us-west-2.amazonaws.com/public/production/v0.4.0/datasets/cities.csv>`__

`or the github repository <https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples/datasets>`__

* `movie_data_random.csv <https://raw.githubusercontent.com/trustedanalytics/atk/master/python-client/trustedanalytics/examples/datasets/movie_data_random.csv>`__
* `cities.csv <https://raw.githubusercontent.com/trustedanalytics/atk/master/python-client/trustedanalytics/examples/datasets/cities.csv>`__


Since the examples will be running in a distributed fashion they will need to be uploaded to HDFS for them to be usable by the examples.

You will need to upload the data sets through the `data catalog <https://github.com/trustedanalytics/platform-wiki/wiki/Using-Data-Catalog-to-download-files-from-S3>`__ to make them available in HDFS. Alternatively you can upload them to HDFS directly if you have shell access to the cluster.

Upload the sample data sets to the `data catalog <https://github.com/trustedanalytics/platform-wiki/wiki/Using-Data-Catalog-to-download-files-from-S3>`__ with the public URLs. Once the transfers are complete take note of the full HDFS path as you will need it to run the examples.

`If you need help navigating the data catalog the instructions can be found here. <https://github.com/trustedanalytics/platform-wiki/wiki/Using-Data-Catalog-to-download-files-from-S3>`__

--------------------
Finding the examples
--------------------

The easiest way to view all the available examples would be to look at them in the `ATK repo <https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples>`__.

They are also available locally once the ATK python client is installed.

To access any of the examples import them like you would any other python module.

**import trustedanalytics.examples.[frame|movie_graph_small|pr] as EXAMPLE_NAME**::


    $ python[2.7]
    >>> import taprootanalytics.examples.frame as frame

----------------
Running Examples
----------------

Once the example is imported execute the run method. The only thing you need to provide is the full HDFS path to the corresponding data set.

-----
Frame
-----
* Uses the cities.csv
* exercises frame operations

frame example::

    $ python[2.7]
    >>> import taprootanalytics.examples.frame as frame
    #after importing the example execute it's run method with the HDFS path from the data catalog
    >>> frame.run("hdfs://FULL_HDFS_PATH")


-----------------
Movie Graph Small
-----------------
* Uses the movie_data_random.csv
* creates a graph from filtered frame data

movie graph example::

    $ python[2.7]
    >>> import taprootanalytics.examples.movie_data_small as movie
    #after importing the example execute it's run method with the HDFS path from the data catalog
    >>> movie.run("hdfs://FULL_HDFS_PATH")


---------
Page Rank
---------
* Uses movie_data_random.csv
* creates a graph and runs the page rank algorithm

page rank example::

    $ python[2.7]
    >>> import taprootanalytics.examples.pr as pr
    #after importing the example execute it's run method with the HDFS path from the data catalog
    >>> pr.run("hdfs://FULL_HDFS_PATH")



