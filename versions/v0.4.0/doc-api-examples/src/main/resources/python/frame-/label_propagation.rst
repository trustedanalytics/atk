Examples
--------
.. only:: html

    .. code::

    input frame (lp.csv)
    "a"        "b"        "c"        "d"
    1,         2,         0.5,       "0.5,0.5"
    2,         3,         0.4,       "-1,-1"
    3,         1,         0.1,       "0.8,0.2"

    script

    ta.connect()
    s = [("a", ta.int32), ("b", ta.int32), ("c", ta.float32), ("d", ta.vector(2))]
    d = "lp.csv"
    c = ta.CsvFile(d,s)
    f = ta.Frame(c)
    r = f.label_propagation("a", "b", "c", "d", "results")
    r['frame'].inspect()
    r['report']

.. only:: latex

    .. code::

        >>> r = f.label_propagation(
        ... srcColName = "a",
        ... destColName  = "b",
        ... weightColName = "c",
        ... srcLabelColName = "d",
        ... resultColName = "resultLabels")
        ... r['frame'].inspect()
        ... r['report']

The expected output is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 600\nNumber of edges: 15716\n\n======LP Configuration======\nlambda: 0.000000\nanchorThreshold: 0.900000\nconvergenceThreshold: 0.000000\nmaxSupersteps: 10\nbidirectionalCheck: false\n\n======Learning Progress======\nsuperstep = 1\tcost = 0.008692\nsuperstep = 2\tcost = 0.008155\nsuperstep = 3\tcost = 0.007809\nsuperstep = 4\tcost = 0.007544\nsuperstep = 5\tcost = 0.007328\nsuperstep = 6\tcost = 0.007142\nsuperstep = 7\tcost = 0.006979\nsuperstep = 8\tcost = 0.006833\nsuperstep = 9\tcost = 0.006701\nsuperstep = 10\tcost = 0.006580'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 600\n
        Number of edges: 15716\n
        \n
        ======LP Configuration======\n
        lambda: 0.000000\n
        anchorThreshold: 0.900000\n
        convergenceThreshold: 0.000000\n
        maxSupersteps: 10\n
        bidirectionalCheck: false\n
        \n
        ======Learning Progress======\n
        superstep = 1\tcost = 0.008692\n
        superstep = 2\tcost = 0.008155\n
        superstep = 3\tcost = 0.007809\n
        superstep = 4\tcost = 0.007544\n
        superstep = 5\tcost = 0.007328\n
        superstep = 6\tcost = 0.007142\n
        superstep = 7\tcost = 0.006979\n
        superstep = 8\tcost = 0.006833\n
        superstep = 9\tcost = 0.006701\n
        superstep = 10\tcost = 0.006580'}

