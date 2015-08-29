Examples
--------
.. only:: html

    .. code::

    input frame (lbp.csv)
    "a"        "b"        "c"        "d"
    1,         2,         0.5,       "0.5,0.5"
    2,         3,         0.4,       "-1,-1"
    3,         1,         0.1,       "0.8,0.2"

    script

    atk.connect()()
    s = [("a", atk.int32), ("b", atk.int32), ("c", atk.float32), ("d", atk.vector(2))]
    d = "lbp.csv"
    c = atk.CsvFile(d,s)
    f = atk.Frame(c)
    r = f.loopy_belief_propagation("a", "b", "c", "d", "results")
    r['frame'].inspect()
    r['report']

.. only:: latex

    .. code::

        >>> r = f.loopy_belief_propagation(
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

        {u'value': u'======Graph Statistics======\nNumber of vertices: 80000 (train: 56123, validate: 15930, test: 7947)\nNumber of edges: 318400\n\n======LBP Configuration======\nmaxSupersteps: 10\nconvergenceThreshold: 0.000000\nanchorThreshold: 0.900000\nsmoothing: 2.000000\nbidirectionalCheck: false\nignoreVertexType: false\nmaxProduct: false\npower: 0.000000\n\n======Learning Progress======\nsuperstep = 1\tavgTrainDelta = 0.594534\tavgValidateDelta = 0.542366\tavgTestDelta = 0.542801\nsuperstep = 2\tavgTrainDelta = 0.322596\tavgValidateDelta = 0.373647\tavgTestDelta = 0.371556\nsuperstep = 3\tavgTrainDelta = 0.180468\tavgValidateDelta = 0.194503\tavgTestDelta = 0.198478\nsuperstep = 4\tavgTrainDelta = 0.113280\tavgValidateDelta = 0.117436\tavgTestDelta = 0.122555\nsuperstep = 5\tavgTrainDelta = 0.076510\tavgValidateDelta = 0.074419\tavgTestDelta = 0.077451\nsuperstep = 6\tavgTrainDelta = 0.051452\tavgValidateDelta = 0.051683\tavgTestDelta = 0.052538\nsuperstep = 7\tavgTrainDelta = 0.038257\tavgValidateDelta = 0.033629\tavgTestDelta = 0.034017\nsuperstep = 8\tavgTrainDelta = 0.027924\tavgValidateDelta = 0.026722\tavgTestDelta = 0.025877\nsuperstep = 9\tavgTrainDelta = 0.022886\tavgValidateDelta = 0.019267\tavgTestDelta = 0.018190\nsuperstep = 10\tavgTrainDelta = 0.018271\tavgValidateDelta = 0.015924\tavgTestDelta = 0.015377'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 80000 (train: 56123, validate: 15930, test: 7947)\n
        Number of edges: 318400\n
        \n
        ======LBP Configuration======\n
        maxSupersteps: 10\n
        convergenceThreshold: 0.000000\n
        anchorThreshold: 0.900000\n
        smoothing: 2.000000\n
        bidirectionalCheck: false\n
        ignoreVertexType: false\n
        maxProduct: false\n
        power: 0.000000\n
        \n
        ======Learning Progress======\n
        superstep = 1\t
            avgTrainDelta = 0.594534\t
            avgValidateDelta = 0.542366\t
            avgTestDelta = 0.542801\n
        superstep = 2\t
            avgTrainDelta = 0.322596\t
            avgValidateDelta = 0.373647\t
            avgTestDelta = 0.371556\n
        superstep = 3\t
            avgTrainDelta = 0.180468\t
            avgValidateDelta = 0.194503\t
            avgTestDelta = 0.198478\n
        superstep = 4\t
            avgTrainDelta = 0.113280\t
            avgValidateDelta = 0.117436\t
            avgTestDelta = 0.122555\n
        superstep = 5\t
            avgTrainDelta = 0.076510\t
            avgValidateDelta = 0.074419\t
            avgTestDelta = 0.077451\n
        superstep = 6\t
            avgTrainDelta = 0.051452\t
            avgValidateDelta = 0.051683\t
            avgTestDelta = 0.052538\n
        superstep = 7\t
            avgTrainDelta = 0.038257\t
            avgValidateDelta = 0.033629\t
            avgTestDelta = 0.034017\n
        superstep = 8\t
            avgTrainDelta = 0.027924\t
            avgValidateDelta = 0.026722\t
            avgTestDelta = 0.025877\n
        superstep = 9\t
            avgTrainDelta = 0.022886\t
            avgValidateDelta = 0.019267\t
            avgTestDelta = 0.018190\n
        superstep = 10\t
            avgTrainDelta = 0.018271\t
            avgValidateDelta = 0.015924\t
            avgTestDelta = 0.015377'}

