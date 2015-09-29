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
    >>> prediction = model.predict(['harry', 'secrets', 'magic', 'harry', 'chamber' 'test'])

The variable *prediction* is a dictionary with three keys:

.. code::

    >>> topics_given_doc = results['topics_given_doc']
    >>> new_words_percentage = results['new_words_percentage']
    >>> new_words_count = results['new_words_count']
    >>> print(prediction)

    {u'topics_given_doc': [0.04150190747884333, 0.7584980925211566], u'new_words_percentage': 20.0, u'new_words_count': 1}

