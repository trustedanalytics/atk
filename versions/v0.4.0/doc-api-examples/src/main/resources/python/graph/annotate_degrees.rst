Examples
--------
Given a graph:

.. code::

    >>> g.query.gremlin('g.V [ 0 .. 1]')

    Out[12]:
       {u'results': [{u'_id': 19456,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 545413,
        u'source': 6961},
       {u'_id': 19968,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 511316,
        u'source': 31599}],
        u'run_time_seconds': 1.822}

    >>> h = g.annotate_degrees('degree')

