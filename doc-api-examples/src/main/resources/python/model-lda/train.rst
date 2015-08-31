Examples
--------

.. code::

    >>> my_model = atk.LdaModel()
    >>> results = my_model.train(frame, 'doc_column_name', 'word_column_name',
    ... 'word_count_column_name', num_topics = 3)

The variable *results* is a dictionary with three keys:

.. code::

    >>> doc_results = results['doc_results']
    >>> word_results = results['word_results']
    >>> report = results['report']

Inspect the results:

.. code::

    >>> doc_results.inspect()
    >>> word_results.inspect()

View the report:

.. code::

    >>> print report

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 12 (doc: 6, word: 6)\nNumber of edges: 12\n\n======LDA Configuration======\nnumTopics: 3\nalpha: 0.100000\nbeta: 0.100000\nconvergenceThreshold: 0.000000\nbidirectionalCheck: false\nmaxIterations: 20\nmaxVal: Infinity\nminVal: -Infinity\nevaluateCost: false\n\n======Learning Progress======\niteration = 1\tmaxDelta = 0.333682\niteration = 2\tmaxDelta = 0.117571\niteration = 3\tmaxDelta = 0.073708\niteration = 4\tmaxDelta = 0.053260\niteration = 5\tmaxDelta = 0.038495\niteration = 6\tmaxDelta = 0.028494\niteration = 7\tmaxDelta = 0.020819\niteration = 8\tmaxDelta = 0.015374\niteration = 9\tmaxDelta = 0.011267\niteration = 10\tmaxDelta = 0.008305\niteration = 11\tmaxDelta = 0.006096\niteration = 12\tmaxDelta = 0.004488\niteration = 13\tmaxDelta = 0.003297\niteration = 14\tmaxDelta = 0.002426\niteration = 15\tmaxDelta = 0.001783\niteration = 16\tmaxDelta = 0.001311\niteration = 17\tmaxDelta = 0.000964\niteration = 18\tmaxDelta = 0.000709\niteration = 19\tmaxDelta = 0.000521\niteration = 20\tmaxDelta = 0.000383'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 12 (doc: 6, word: 6)\n
        Number of edges: 12\n
        \n
        ======LDA Configuration======\n
        numTopics: 3\n
        alpha: 0.100000\n
        beta: 0.100000\n
        convergenceThreshold: 0.000000\n
        bidirectionalCheck: false\n
        maxIterations: 20\n
        maxVal: Infinity\n
        minVal: -Infinity\n
        evaluateCost: false\n
        \n
        ======Learning Progress======\n
        iteration = 1\tmaxDelta = 0.333682\n
        iteration = 2\tmaxDelta = 0.117571\n
        iteration = 3\tmaxDelta = 0.073708\n
        iteration = 4\tmaxDelta = 0.053260\n
        iteration = 5\tmaxDelta = 0.038495\n
        iteration = 6\tmaxDelta = 0.028494\n
        iteration = 7\tmaxDelta = 0.020819\n
        iteration = 8\tmaxDelta = 0.015374\n
        iteration = 9\tmaxDelta = 0.011267\n
        iteration = 10\tmaxDelta = 0.008305\n
        iteration = 11\tmaxDelta = 0.006096\n
        iteration = 12\tmaxDelta = 0.004488\n
        iteration = 13\tmaxDelta = 0.003297\n
        iteration = 14\tmaxDelta = 0.002426\n
        iteration = 15\tmaxDelta = 0.001783\n
        iteration = 16\tmaxDelta = 0.001311\n
        iteration = 17\tmaxDelta = 0.000964\n
        iteration = 18\tmaxDelta = 0.000709\n
        iteration = 19\tmaxDelta = 0.000521\n
        iteration = 20\tmaxDelta = 0.000383'}

