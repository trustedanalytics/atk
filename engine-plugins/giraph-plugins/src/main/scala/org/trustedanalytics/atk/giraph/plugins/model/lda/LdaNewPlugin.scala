/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.giraph.plugins.model.lda

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Latent Dirichlet allocation - create a 'new' instance of this model
 */
@PluginDoc(oneLine = "Creates Latent Dirichlet Allocation model",
  extended = """
**Topic Modeling with Latent Dirichlet Allocation**

:term:`Topic modeling` algorithms are a class of statistical approaches to
partitioning items in a data set into subgroups.
As the name implies, these algorithms are often used on corpora of textual
data, where they are used to group documents in the collection into
semantically-meaningful groupings.
For an overall introduction to topic modeling, the reader might refer to the
work of David Blei and Michael Jordan, who are credited with creating and
popularizing topic modeling in the machine learning community.
In particular, Blei's 2011 paper provides a nice introduction,
and is freely-available online [#LDA1]_ .

|LDA| is a commonly-used algorithm for topic modeling, but, more broadly,
is considered a dimensionality reduction technique.
It contrasts with other approaches (for example, latent semantic indexing), in
that it creates what's referred to as a generative probabilistic model |EM| a
statistical model that allows the algorithm to generalize its approach to topic
assignment to other, never-before-seen data points.
For the purposes of exposition, we'll limit the scope of our discussion of
|LDA| to the world of natural language processing, as it has an intuitive use
there (though |LDA| can be used on other types of data).
In general, |LDA| represents documents as random mixtures over topics in the
corpus.
This makes sense because any work of writing is rarely about a single subject.
Take the case of a news article on the President of the United States of
America's approach to healthcare as an example.
It would be reasonable to assign topics like President, USA, health insurance,
politics, or healthcare to such a work, though it is likely to primarily
discuss the President and healthcare.

|LDA| assumes that input corpora contain documents pertaining to a given number
of topics, each of which are associated with a variety of words, and that each
document is the result of a mixture of probabilistic samplings: first over the
distribution of possible topics for the corpora, and second over the list of
possible words in the selected topic.
This generative assumption confers one of the main advantages |LDA| holds over
other topic modeling approaches, such as probabilistic and regular |LSI|.
As a generative model, |LDA| is able to generalize the model it uses to
separate documents into topics to documents outside the corpora.
For example, this means that using |LDA| to group online news articles into
categories like Sports, Entertainment, and Politics, it would be possible to
use the fitted model to help categorize newly-published news stories.
Such an application is beyond the scope of approaches like |LSI|.
What's more, when fitting an |LSI| model, the number of parameters that have
to be estimated scale linearly with the number of documents in the corpus,
whereas the number of parameters to estimate for an |LDA| model scales with the
number of topics |EM| a much lower number, making it much better-suited to working
with large data sets.

**The Typical Latent Dirichlet Allocation Workflow**

Although every user is likely to have his or her own habits and preferred
approach to topic modeling a document corpus, there is a general workflow that
is a good starting point when working with new data.
The general steps to the topic modeling with |LDA| include:

1. Data preparation and ingest
#. Assignment to training or testing partition
#. Graph construction
#. Training |LDA|
#. Evaluation
#. Interpretation of results

**Data preparation and ingest**

Most topic modeling workflows involve several data pre-processing and cleaning
steps.
Depending on the characteristics of the data being analyzed, there are
different best-practices to use here, so it's important to be familiar with
the standard procedures for analytics in the domain from which the text
originated.
For example, in the biomedical text analytics community, it is common practice
for text analytics workflows to involve pre-processing for identifying negation
statements (Chapman et al., 2001 [#LDA2]_ ).
The reason for this is many analysts in that domain are examining text for
diagnostic statements |EM| thus, failing to identify a negated statement in which
a disease is mentioned could lead to undesirable false-positives, but this
phenomenon may not arise in every domain.
In general, both stemming and stop word filtering are recommended steps for
topic modeling pre-processing.
Stemming refers to a set of methods used to normalize different tenses and
variations of the same word (for example, stemmer, stemming, stemmed, and
stem).
Stemming algorithms will normalize all variations of a word to one common form
(for example, stem).
There are many approaches to stemming, but the Porter Stemming (Porter, 2006
[#LDA3]_ ) is one of the most commonly-used.

Removing common, uninformative words, or stop word filtering, is another
commonly-used step in data pre-processing for topic modeling.
Stop words include words like *the*, *and*, or *a*, but the full list of
uninformative words can be quite long and depend on the domain producing the
text in question.
Example stop word lists online [#LDA4]_ can be a great place to start, but
being aware of the best-practices in the applicalbe field is necessary to
expand upon these.

There may be other pre-processing steps needed, depending on the type of text
being worked with.
Punctuation removal is frequently recommended, for example.
To determine what's best for the text being analyzed, it helps to understand a
bit about what how |LDA| analyzes the input text.
To learn the topic model, |LDA| will typically look at the frequency of
individual words across documents, which are determined based on
space-separation.
Thus, each word will be interpreted independent of where it occurs in a
document, and without regard for the words that were written around it.
In the text analytics field, this is often referred to as a *bag of words*
approach to tokenization, the process of separating input text into
composite features to be analyzed by some algorithm.
When choosing pre-processing steps, it helps to keep this in mind.
Don't worry too much about removing words or modifying their format |EM| you're
not manipulating your data!
These steps simply make it easier for the topic modeling algorithm to find the
latent topics that comprise your corpus.

**Assignment to training or testing partition**

The random assignment to training and testing partitions is an important step
in most every machine learning workflow.
It is common practice to withhold a random selection of one's data set for the
purpose of evaluating the accuracy of the model that was learned from the
training data.
The results of this evaluation allow the user to confidently speak about the
generalizability of the trained model.
When speaking in these terms, be cautious that you only discuss
generalizability to the broader population from which your data was originally
obtained.
If a topic model is trained on neuroscience-related publications,
for example, evaluating the model on other neuroscience-related publications
is valid.
It would not be valid to discuss the model's ability to work on documents from
other domains.

There are various schools of thought for how to assign a data set to training
and testing collections, but all agree that the process should be random.
Where analysts disagree is in the ratio of data to be assigned to each.
In most situations, the bulk of data will be assigned to the training
collection, because the more data that can be used to train the algorithm,
the better the resultant model will typically be.
It's also important that the testing collection have sufficient data to
be able to reflect the characteristics of the larger
population from which it was drawn (this becomes an important issue when
working with data sets with rare topics, for example).
As a starting point, many people will use a 90%/10% training/test collection
split, and modify this ratio based on the characteristics of the documents
being analyzed.

**Graph construction**

|PACKAGE| uses a bipartite graph, to learn an |LDA| topic model.
This graph contains vertices in two columns.
The left-hand column contains unique ids, each corresponding to a document in
the training collection, while the right-hand column contains unique ids
corresponding to each word in the entire training set, following any
pre-processing steps that were used.
Connections between these columns, or edges, denote the number of times a
particular word appears in a document, with the weight on the edge in question
denoting the number of times the word was found there.
After graph construction, many analysts choose to normalize the weights using
one of a variety of normalization schemes.
One approach is to normalize the weights to sum to 1, while another is to use
an approach called term frequency-inverse document frequency (tfidf), where the
resultant weights are meant to reflect how important a word is to a document in
the corpus.
Whether to use normalization |EM| or what technique to use |EM| is an open question,
and will likely depend on the characteristics of the text being analyzed.
Typical text analytics experiments will try a variety of approaches on a small
subset of the data to determine what works best.

.. only:: latex

    See `Figure 1 <ds_mlal_lda_fig_1>`_.

.. _ds_mlal_lda_fig_1:

.. figure:: /ds_mlal_lda_1.*
    :align: center

    Figure 1 - Example layout of a bipartite graph for LDA.

    The left-hand column contains one vertex for each document in the input
    corpus, while the right-hand column contains vertices for each unique word
    found in them.
    Edges connecting left- and right-hand columns denote the number of times
    the word was found in the document the edge connects.
    The weights of the edges used in this example were not normalized.

**Training the Model**

In using |LDA|, we are trying to model a document collection in terms of topics
:math:`\beta_{1:K}`, where each :math:`\beta_{K}` describes a distribution
over the set of words in the training corpus.
Every document :math:`d`, then, is a vector of proportions :math:`\theta_d`,
where :math:`\theta_{d,k}` is the proportion of the :math:`d^{th}` document for
topic :math:`k`.
The topic assignment for document :math:`d` is :math:`z_{d}`, and
:math:`z_{d,n}` is the topic assignment for the :math:`n^{th}` word
in document :math:`d`.
The words observed in document :math:`d` are :math"`w_{d}`, and
:math:`w_{d,n}` is the :math:`n^{th}` word in document :math:`d`.
The generative process for |LDA|, then, is the joint distribution of hidden and
observed values

.. math::

    p(\beta_{1:K},\theta_{1:D},z_{1:D},w_{1:D} )=\prod_{i=1}^{K} p(\beta_i) \
    \prod_{i=1}^{D} p(\theta_d) \left(\sideset{_{}^{}}{_{n=1}^N}\prod_{}^{} \
    p\left(z_{d,n} | \theta_{d} \right)p\left(w_{d,n} | \beta_{1:K},z_{d,n} \
    \right) \right)

This distribution depicts several dependencies: topic assignment
:math:`z_{d,n}` depends on the topic proportions :math:`\theta_d`,
and the observed word :math:`w_{d,n}` depends on topic assignment
:math:`z_{d,n}` and all the topics :math:`\beta_{1:K}`, for example.
Although there are no analytical solutions to learning the |LDA| model, there
are a variety of approximate solutions that are used, most of which are based
on Gibbs Sampling (for example, Porteous et al., 2008 [#LDA5]_ ).
The |PACKAGE| uses an implementation related to this.
We refer the interested reader to the primary source on this approach to learn
more (Teh et al., 2006 [#LDA6]_ ).

**Evaluation**

As with every machine learning algorithm, evaluating the accuracy of the model
that has been obtained is an important step before interpreting the results.
With many types of algorithms, the best practices in this step are
straightforward |EM| in supervised classification, for example, we know the true
labels of the data being classified, so evaluating performance can be as simple
as computing the number of errors, calculating receiver operating
characteristic, or F1 measure.
With topic modeling, the situation is not so straightforward.
This makes sense, if we consider with |LDA| we're using an algorithm to blindly
identify logical subgroupings in our data, and we don't *a priori* know the
best grouping that can be found.
Evaluation, then, should proceed with this in mind, and an examination of
homogeneity of the words comprising the documents in each grouping is often
done.
This issue is discussed further in Blei's 2011 introduction to topic modeling
[#LDA7]_ .
It is of course possible to evaluate a topic model from a statistical
perspective using our hold-out testing document collection |EM| and this is a
recommended best practice |EM| however, such an evaluation does not assess the
topic model in terms of how they are typically used.

**Interpretation of results**

After running |LDA| on a document corpus, users will typically examine the top
:math:`n` most frequent words that can be found in each grouping.
With this information, one is often able to use their own domain expertise to
think of logical names for each topic (this situation is analogous to the step
in principal components analysis, wherein statisticians will think of logical
names for each principal component based on the mixture of dimensions each
spans).
Each document, then, can be assigned to a topic, based on the mixture of topics
it has been assigned.
Recall that |LDA| will assign each document a set of probabilities
corresponding to each possible topic.
Researchers will often set some threshold value to make a categorical judgment
regarding topic membership, using this information.

.. rubric:: footnotes

.. [#LDA1] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf
.. [#LDA2] http://www.sciencedirect.com/science/article/pii/S1532046401910299
.. [#LDA3] http://tartarus.org/~martin/PorterStemmer/index.html
.. [#LDA4] http://www.textfixer.com/resources/common-english-words.txt
.. [#LDA5] http://www.ics.uci.edu/~newman/pubs/fastlda.pdf
.. [#LDA6] http://machinelearning.wustl.edu/mlpapers/paper_files/NIPS2006_511.pdf
.. [#LDA7] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf
""")
class LdaNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    val models = engine.models
    models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:lda")))
  }
}
