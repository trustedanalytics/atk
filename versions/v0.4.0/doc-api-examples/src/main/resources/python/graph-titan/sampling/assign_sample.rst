Examples
--------
Given *my_graph* is a Graph object accessing a graph with data:

.. only:: html

    .. code::

        >>> my_graph = ta.get_graph('my_graph')
        >>> my_graph.query.gremlin("g.V [0..2]")
        {u'results': [{u'_vid': 4, u'source': 3, u'_type': u'vertex', u'_id': 30208, u'_label': u'vertex'}, {u'_vid': 3, u'source': 2, u'_type': u'vertex', u'_id': 19992, u'_label': u'vertex'}, {u'_vid': 1, u'source': 1, u'_type': u'vertex', u'_id': 23384, u'_label': u'vertex'}], u'run_time_seconds': 2.165}

.. only:: latex

    .. code::

        >>> my_graph = ta.get_graph('my_graph')
        >>> my_graph.query.gremlin("g.V [0..2]")
        {u'results':
          [{u'_vid': 4,
            u'source': 3,
            u'_type':
            u'vertex',
            u'_id': 30208,
            u'_label':
            u'vertex'},
           {u'_vid': 3,
            u'source': 2,
            u'_type':
            u'vertex',
            u'_id': 19992,
            u'_label':
            u'vertex'},
           {u'_vid': 1,
            u'source': 1,
            u'_type':
            u'vertex',
            u'_id': 23384,
            u'_label':
            u'vertex'}],
            u'run_time_seconds': 2.165}

Append a new property *sample_bin* to every vertex in the graph;
Assign the value in the new vertex property to "train", "test", or "validate":

.. code::

    >>> my_graph.sampling.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])

Now the vertices of *my_graph* have a new vertex property named "sample_bin"
and each vertex contains one of the values "train", "test", or "validate".
Existing properties are unaffected.

.. only:: html

    .. code::

        >>> my_graph.query.gremlin("g.V [0..2]")
        {u'results': [
        {u'_vid': 4, u'source': 3, u'_type': u'vertex', u'_id': 30208, u'_label': u'vertex', u'sample_bin': u'train'},
        {u'_vid': 3, u'source': 2, u'_type': u'vertex', u'_id': 19992, u'_label': u'vertex', u'sample_bin': u'test'},
        {u'_vid': 1, u'source': 1, u'_type': u'vertex', u'_id': 23384, u'_label': u'vertex', u'sample_bin': u'validate'}],
         u'run_time_seconds': 2.165}

.. only:: latex

    .. code::

        >>> my_graph.query.gremlin("g.V [0..2]")
        {u'results': [
        {u'_vid': 4,
         u'source': 3,
         u'_type':
         u'vertex',
         u'_id': 30208,
         u'_label':
         u'vertex',
         u'sample_bin':
         u'train'},
        {u'_vid': 3,
         u'source': 2,
         u'_type':
         u'vertex',
         u'_id': 19992,
         u'_label':
         u'vertex',
         u'sample_bin':
         u'test'},
        {u'_vid': 1,
         u'source': 1,
         u'_type':
         u'vertex',
         u'_id': 23384,
         u'_label':
         u'vertex',
         u'sample_bin':
         u'validate'}],
         u'run_time_seconds': 2.165}

