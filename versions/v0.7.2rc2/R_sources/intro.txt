.. _intro:

=================
Technical Summary
=================

.. contents:: Table of Contents
    :local:
    :backlinks: none

--------
Overview
--------

|PACKAGE| is a platform that simplifies applying
:term:`machine learning` to big data for superior knowledge discovery and
predictive modeling across a wide variety of use cases and solutions.
|PACKAGE| provides an analytics pipeline (|ATK|) spanning feature engineering, graph
construction and various types of machine learning using an extensible, modular framework.
All functionality operates at full scale, yet are accessed using a higher level
Python data science programming abstraction to significantly ease the
complexity of cluster computing and parallel processing.
The platform is extensible through a plugin architecture that allows
incorporating the full range of analytics and machine learning for any solution
need in a unified workflow.  Researchers can avoid the overhead of
understanding, integrating, and inefficiently iterating across a diversity of
formats and interfaces.


--------------------------------------------
Python and Data Frame User Interface Summary
--------------------------------------------

|ATK| utilizes Python abstractions similar to popular desktop machine learning
solutions (e.g. Pandas, SciKit Learn) to make programming fully scalable big data
analytic workflows using Spark/Hadoop clusters familiar and accessible.
The scalable data frame representation is familiar and intuitive to data
researchers compared to low level HDFS file and Spark RDD formats.
|ATK| provides an extensive library to manipulate the data frames for
feature engineering and exploration, such as joins and aggregations.
User-defined transformations and filters can be written in Python and applied
to terabytes of data using distributed processing.
Machine learning algorithms are also invoked as higher-level API
(Application Programming Interface) abstractions, making model development,
like creating parallel recommender systems or training clustering models,
accessible to a broad population of researchers possessing mainstream data
science programming skills.
For more information, see the section on :doc:`process flow </ds_pflw>`
and the `Python website <http://www.python.org>`__.


-----------------------
Graph Analytics Summary
-----------------------

In addition to enabling use of entity-based data representations and
algorithms, scalable graph analytics are provided for uncovering central
influences and communities in the data set.
Graph representations are broadly useful, for example to link disparate data
using arbitrary edge types, and then analyze the connections for powerful
predictive signals that can otherwise be missed with entity-based methods.
Working with graph representations can often be more intuitive and
computationally efficient for data sets where the connections between data
observations are more numerous and more important than the data points alone.
|ATK| offers a representation of graph data as fully-scalable property
graph objects with vertices, edges, and associated properties.
The toolkit brings together the capabilities to create
and analyze graph objects, including engineering features, linking data,
performing rich traversal queries, and applying graph-based algorithms.
Because data scientists often need to iterate using both graph and
frame representations (for example, applying a clustering algorithm to a vertex
list with features developed using graph analytics), |ATK| provides the
seamless ability to move between both data representations.


------------------------
Machine Learning Summary
------------------------

The toolkit provides algorithms for supervised, unsupervised, and
semi-supervised machine learning using both entity and graphical machine
learning tools.
Graph machine learning algorithms such as label propagation and loopy belief
propagation, exploit the connections in the graph structure and provide
powerful new methods of labeling or classifying graph data.
Examples of other machine learning capabilities provided include recommender
systems using alternating least squares and conjugate gradient descent, topic
modeling using Latent Dirichlet Allocation, clustering using K-means, and
classification using logistic regression.
See the section on :doc:`machine learning </ds_ml>` and the
:doc:`API </python_api/index>` for further information.

--------------
Scoring Engine
--------------

The scoring engine produces predictions from inputs using a previously trained 
machine learning model. Once a model has been trained and evaluated, it can be 
deployed easily into production via the scoring engine.

---------------
Plugins Summary
---------------

In addition to the extensive set of capabilities provided, the platform is
extensible using a plugin architecture.
This allows developers to incorporate graph analytical tools into the existing
range of machine learning abilities, expanding the capabilities of |PACKAGE|
for new problem solutions.
Plugins are developed using a thin Scala wrapper, and the |ATK| framework
automatically generates a Python presentation for those added functions.
Plug-ins can be used for a range of purposes, such as developing custom
algorithms for specialized data types, building custom transformations for
commonly used functions to get higher performance than a |UDF|, or integrating
other tools to further unify the workflow.
See the :doc:`Plugin Authoring Guide </dev_plug>` for more information.
