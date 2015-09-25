Examples
--------
Get the first two outgoing edges of the vertex whose source equals 5767244:

.. code::

    >>> mygraph = ta.get_graph("mytitangraph")
    >>> results = mygraph.query.gremlin("g.V('source', 5767244).outE[0..1]")
    >>> print results["results"]

The expected output is a list of edges in GraphSON format:

.. only:: html

    .. code::

        [{u'_label': u'edge', u'_type': u'edge', u'_inV': 1381202500, u'weight': 1, u'_outV': 1346400004, u'_id': u'fDEQC9-1t7m96-1U'},{u'_label': u'edge', u'_type': u'edge', u'_inV': 1365600772, u'weight': 1, u'_outV': 1346400004, u'_id': u'frtzv9-1t7m96-1U'}]

.. only:: latex

    .. code::

        [{u'_label': u'edge',
          u'_type': u'edge',
          u'_inV': 1381202500,
          u'weight': 1,
          u'_outV': 1346400004,
          u'_id': u'fDEQC9-1t7m96-1U'},
         {u'_label': u'edge',
          u'_type': u'edge',
          u'_inV': 1365600772,
          u'weight': 1,
          u'_outV': 1346400004,
          u'_id': u'frtzv9-1t7m96-1U'}]

Get the count of incoming edges for a vertex:

.. code::

    >>> results = mygraph.query.gremlin("g.V('target', 5767243).inE.count()")
    >>> print results["results"]

The expected output is:

.. code::

    [4]

Get the count of name and age properties from vertices:

.. code::

    >>> results = mygraph.query.gremlin("g.V.transform{[it.name, it.age]}[0..10])")
    >>> print results["results"]

The expected output is:

.. code::

    [u'["alice", 29]', u'[ "bob", 45 ]', u'["cathy", 34 ]']

