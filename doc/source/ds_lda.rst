.. _ds_lda:

===========================
Latent Dirichlet Allocation
===========================

LDA, short for Latent Dirichlet Allocation, performs topic modeling, which is a form of clustering.
For example, given a number of documents, LDA can group the texts on similar topics together based on whether they contain similar words. LDA is an unsupervised algorithm, meaning that the groups are created based on the similarity to each other, rather than by comparing them to an idealized or standardized dataset.


Setup
-----

Establish a connection to the ATK Rest Server.
This handle will be used for the remainder of the script.

Get server URL and credentials file from the TAP administrator.

.. code::
   atk_server_uri = os.getenv("ATK_SERVER_URI", ia.server.uri)
   credentials_file = os.getenv("ATK_CREDENTIALS", "")

Set the server, and use the credentials to connect to the ATK.

.. code::
   ia.server.uri = atk_server_uri
   ia.connect(credentials_file)

--------
Workflow
--------

The standard workflow is: build a frame, build a model, train the model on the frame, predict on the model.
Note there is no metrics. Evaluating unsupervised machine learning results can be difficult.

Construct a Frame
-----------------

Construct a frame to be uploaded, this is done using Python lists uploaded to the server

In this example, there are 3 papers on 2 topics, with 4 words each, which each appear 2 times

.. code::

   rows_frame = ia.UploadRows([["paper1", "word11", 2],
                               ["paper1", "word12", 2],
                               ["paper1", "word13", 2],
                               ["paper1", "word14", 2],

                               ["paper2", "word11", 2],
                               ["paper2", "word12", 2],
                               ["paper2", "word13", 2],
                               ["paper2", "word14", 2],

                               ["paper3", "word11", 2],
                               ["paper3", "word22", 2],
                               ["paper3", "word23", 2],
                               ["paper3", "word24", 2]],
                               [("paper", str),
                                ("word", str),
                                ("count", ia.int32)])

   frame = ia.Frame(rows_frame)

   print frame.inspect()

Build a Model
-------------

.. code::

   lda_model = ia.LdaModel()

Train the Model
---------------

This LDA model will be trained using the frame above.

[FIX THIS]Give the model the papers to train topics on, and words associated with those papers, and the count of words in a paper.
The final argument is the number of topics to search for.

.. code::

   lda_model.train(frame, "paper", "word", "count", num_topics=2)

Predict on the Model
--------------------

Using the trained model, predict the words of two new papers, and show that they are in different topics. A new paper is a list of words and counts of those words

.. code::

   cluster1 = lda_model.predict(["word11", "word13"])
   cluster2 = lda_model.predict(["word21", "word23"])
   print cluster1
   print cluster2

   self.assertNotEqual(cluster1, cluster2)


