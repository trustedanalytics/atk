.. _ds_ml.rst:

.. index::
    single: machine learning

================
Machine Learning
================

:term:`Machine learning` is the study of constructing algorithms that can learn
from data.

When someone uses a search engine to perform a query, they are returned a
ranked list of websites, ordered according to predicted relevance.
Ranking these sites is typically done using page content, as well as the
relevance of other sites that link to a particular page.
Machine learning is used to automate this process, allowing search engine
companies to scale this process up to billions of potential web pages.

Online retailers often use a machine learning algorithm called collaborative
filtering to suggest products users might be interested in purchasing.
These suggestions are produced dynamically and without the use of a specific
input query, so retailers use a customer's purchase and browsing history,
along with those of customers with whom shared interests can be identified.
Implementations of collaborative filtering enable these recommendations to
be done automatically, without directly involving analysts.

There are many other problems that are amenable to :term:`machine learning`
solutions.
Translation of text for example is a difficult issue.
A corpus of pre-translated text can be used to teach an algorithm a mapping
from one language to another.

----------
Algorithms
----------

.. toctree::
    :maxdepth: 2

    ds_mlal_0

.. index::
    single: supervised
    single: unsupervised
    single: semi-supervised
    single: binary logistic regression
    single: classification
    single: prediction

-----------
Supervision
-----------

The |PACKAGE| incorporates supervised, unsupervised, and
semi-supervised machine learning algorithms.
Supervised algorithms are used to learn the relationship between features in
a dataset and some labeling schema, such as is in classification.
For example, binary logistic regression builds a model for relating a linear
combination of input features (e.g., high and low temperatures for a
collection of days) to a known binary label (e.g., whether or not someone
went for a trail run on that day).
Once the relationship between temperature and running activity is learned,
then the model can be used to make predictions about new running activity,
given the days temperatures.
Unsupervised machine learning algorithms are used to find patterns or
groupings in data for which class labels are unknown.
For example, given a data set of observations about flowers (e.g., petal
length, petal width, sepal length, and sepal width), an unsupervised
clustering algorithm could be used to cluster observations according to
similarity.
Then, a researcher could look for reasonable patterns in the groupings, such
as "similar species appear to cluster together."
Semi-supervised learning is the natural combination of these two classes of
algorithms, in which unlabeled data are supplemented with smaller amounts of
labeled data, with the goal of increasing the accuracy of learning.
For more information on these approaches, the respective wikipedia entries
to these approaches provide an easy-to-read overview of their strengths and
limitations.

---------------
Other Resources
---------------

There is plenty of literature on :term:`machine learning` for those who want to
gain a more thorough understanding of it.
We recommend: `Introduction to Machine Learning`_ and `Wikipedia\: Machine
Learning`_.
You might find this link helpful as well: `Everything You Wanted to Know About
Machine Learning, But Were Too Afraid To Ask (Part Two)`_.

.. _Introduction to Machine Learning: http://alex.smola.org/drafts/thebook.pdf
.. _Wikipedia\: Machine Learning: http://en.wikipedia.org/wiki/Machine_learning
.. _Everything You Wanted to Know About Machine Learning, But Were Too Afraid To Ask (Part Two): http://blog.bigml.com/2013/02/21/everything-you-wanted-to-know-about-machine-learning-but-were-too-afraid-to-ask-part-two/
.. _Wikipedia\: Machine Learning / Algorithm Types: http://en.wikipedia.org/wiki/Machine_learning#Algorithm_types

