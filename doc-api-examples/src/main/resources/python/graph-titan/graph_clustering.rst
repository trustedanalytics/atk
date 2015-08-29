Examples
--------
The data file sample_graph.txt is a file in the following format: src, dest,
distance:

.. code::

    1, 2, 1.5f
    2, 1, 1.5f
    2, 3, 1.5f
    3, 2, 1.5f
    1, 3, 1.5f
    3, 1, 1.5f

The edge column name should be passed in as an argument to the plug-in.

.. code::

    >>> import trustedanalytics as atk
    >>> atk.connect()
    >>> my_graph = atk.TitanGraph([src, dest, dist], "sample_graph")
    >>> my_graph.graph_clustering("dist")

The expected output (new vertices) can be queried:

.. only:: html

    .. code::

        >>> my_graph.query.gremlin('g.V.map(\'id\', \'vertex\', \'_label\', \'name\',\'count\')')

.. only:: latex

    .. code::

        >>> my_graph.query.gremlin('g.V.map(\'id\', \'vertex\', \'_label\',
        ... \'name\',\'count\')')

Snippet output for the above query will look like this:

.. only:: html

    .. code::

        {u'results': [u'{id=18432, count=null, _label=null, vertex=29, name=null}', u'{id=24576, count=null, _label=null, vertex=22, name=null}', u'{id=27136, count=null, _label=2, vertex=null, name=21944_25304}'

.. only:: latex

    .. code::

        {u'results':
         [u'{id=18432, count=null, _label=null, vertex=29, name=null}',
          u'{id=24576, count=null, _label=null, vertex=22, name=null}',
          u'{id=27136, count=null, _label=2, vertex=null, name=21944_25304}'

where:

    ``24576`` - represents an initial node
    ``27136`` - represents a meta-node of 2 nodes (as per _label value)





