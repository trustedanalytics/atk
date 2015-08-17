Examples
--------
Given a directed graph with three nodes and two edges like this:

.. only:: html

    .. code::

        >>> g.query.gremlin('g.V')
        Out[23]: {u'results': [{u'_id': 28304, u'_label': u'vertex', u'_type': u'vertex', u'_vid': 4, u'source': 2}, {u'_id': 21152, u'_label': u'vertex', u'_type': u'vertex', u'_vid': 1, u'source': 1}, {u'_id': 28064, u'_label': u'vertex', u'_type': u'vertex', u'_vid': 3, u'source': 3}], u'run_time_seconds': 1.245}

        >>> g.query.gremlin('g.E')
        Out[24]: {u'results': [{u'_eid': 3, u'_id': u'34k-gbk-bth-lnk', u'_inV': 28064, u'_label': u'edge', u'_outV': 21152, u'_type': u'edge', u'weight': 0.01}, {u'_eid': 4, u'_id': u'1xw-gbk-bth-lu8', u'_inV': 28304, u'_label': u'edge', u'_outV': 21152, u'_type': u'edge', u'weight': 0.1}], u'run_time_seconds': 1.359}

        >>> h = g.annotate_weighted_degrees('weight',  edge_weight_property = 'weight')

.. only:: latex

    .. code::

        >>> g.query.gremlin('g.V')
        Out[23]:
        {u'results': [{u'_id': 28304,
         u'_label': u'vertex',
         u'_type': u'vertex',
         u'_vid': 4,
         u'source': 2},
        {u'_id': 21152,
         u'_label': u'vertex',
         u'_type': u'vertex',
         u'_vid': 1,
         u'source': 1},
        {u'_id': 28064,
         u'_label': u'vertex',
         u'_type': u'vertex',
         u'_vid': 3,
         u'source': 3}],
         u'run_time_seconds': 1.245}

        >>> g.query.gremlin('g.E')
        Out[24]:
        {u'results': [{u'_eid': 3,
         u'_id': u'34k-gbk-bth-lnk',
         u'_inV': 28064,
         u'_label': u'edge',
         u'_outV': 21152,
         u'_type': u'edge',
         u'weight': 0.01},
        {u'_eid': 4,
         u'_id': u'1xw-gbk-bth-lu8',
         u'_inV': 28304,
         u'_label': u'edge',
         u'_outV': 21152,
         u'_type': u'edge',
         u'weight': 0.1}],
         u'run_time_seconds': 1.359}

        >>> h = g.annotate_weighted_degrees(
        ...        'weight',
        ...        edge_weight_property = 'weight')

