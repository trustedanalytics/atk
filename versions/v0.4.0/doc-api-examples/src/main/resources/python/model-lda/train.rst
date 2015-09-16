Examples
--------

Inspect the input frame:

.. code::
    >>> frame.inspect()

    doc_id:unicode   word_id:unicode   word_count:int64
    \-------------------------------------------------------\
      nytimes          harry                            3
      nytimes          economy                         35
      nytimes          jobs                            40
      nytimes          magic                            1
      nytimes          realestate                      15
      nytimes          movies                           6
      economist        economy                         50
      economist        jobs                            35
      economist        realestate                      20
      economist        movies                           1

.. code::

    >>> my_model = ta.LdaModel()
    >>> results = my_model.train(frame, 'doc_id', 'word_id', 'word_count', max_iterations = 3, num_topics = 2)

The variable *results* is a dictionary with four keys:

.. code::

    >>> topics_given_doc = results['topics_given_doc']
    >>> word_given_topics = results['word_given_topics']
    >>> topics_given_word = results['topics_given_word']
    >>> report = results['report']


Inspect the results:

.. code::
    >>> print("conditional probability of topics given document")
    >>> topics_given_doc.inspect()

     doc_id:unicode         topic_probabilities:vector(2)
    \--------------------------------------------------------------\
      nytimes           [0.6411692639409163, 0.3588307360590836]
      economist        [0.8729037921033002, 0.12709620789669976]
      harrypotter      [0.04037038771254577, 0.9596296122874542]

    >>> print("conditional probability of word given topics")
    >>> word_given_topics.inspect()

     word_id:unicode          topic_probabilities:vector(2)
    \-----------------------------------------------------------------\
      jobs                [0.310153576632259, 0.15771353529047744]
      realestate        [0.19982350094988224, 0.026879558761907126]
      secrets           [0.002344603492542889, 0.16832490819830945]
      magic             [0.015459535145939628, 0.16833906458965853]
      chamber           [0.008569051106470452, 0.10657403682025736]
      economy            [0.4842227935973047, 0.06458045349269073]
      harry             [0.019215463609876204, 0.23279687893102033]
      movies            [0.03728831237906273, 0.008577561181278793]

    >>> print("conditional probability of topics given word")
    >>> topics_given_word.inspect()

     word_id:unicode         topic_probabilities:vector(2)
    \----------------------------------------------------------------\
      chamber           [0.060256136055438406, 0.9397438639445616]
      movies             [0.797036385540048, 0.2029636144599522]
      secrets           [0.008569949938887081, 0.9914300500611131]
      magic              [0.0704558957152857, 0.9295441042847143]
      harry             [0.06424184587002194, 0.9357581541299779]
      realestate        [0.8666980184047205, 0.13330198159527962]
      jobs              [0.6285123369605498, 0.37148766303945036]
      economy            [0.8664742287086458, 0.1335257712913541]

View the report:

.. code::

    >>> print report

    ======Graph Statistics======
    Number of vertices: 11 (doc: 3, word: 8)
    Number of edges: 32

    ======LDA Configuration======
    numTopics: 2
    alpha: 0.100000
    beta: 0.100000
    convergenceThreshold: 0.001000
    maxIterations: 3
    evaluateCost: false

    ======Learning Progress======
    iteration = 1	maxDelta = 0.677352
    iteration = 2	maxDelta = 0.173309
    iteration = 3	maxDelta = 0.181216

