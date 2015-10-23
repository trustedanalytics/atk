Examples
--------
    <skip>
    >>> s = [("a", ta.int32), ("b", ta.int32), ("c", ta.float32), ("d", ta.vector(2))]
    >>> rows = [ [1, 2, 0.5,"0.5,0.5"], [2, 3, 0.4, "0.8,0.2"], [3, 1, 0.1, "0.8,0.2"] ]
    >>> f = ta.Frame(ta.UploadRows (rows, s))
    <progress>

    >>> f.inspect()
    [#]    a   b   c       d
    =========================
    [0]    1   2  0.5 "0.5,0.5"
    [1]    2   3  0.4 "-1,-1"
    [2]    3   1  0.1 "0.8,0.2"

    >>> r = f.loopy_belief_propagation("a", "b", "c", "d", "results")
    <progress>

    >>> r['frame'].inspect()
    [#]    features
    ===============
     [1] "0.5,0.5"
     [2] "0.6,0.4"
     [3] "0.8,0.2"

    >>> r['report']
     ======Graph Statistics======
     Number of vertices: 80000 (train: 56123, validate: 15930, test: 7947)
     Number of edges: 318400

     ======LBP Configuration======
         maxSupersteps: 10
         convergenceThreshold: 0.000000
         anchorThreshold: 0.900000
         smoothing: 2.000000
         bidirectionalCheck: false
         ignoreVertexType: false
         maxProduct: false
         power: 0.000000

     ======Learning Progress======
         superstep = 1
             avgTrainDelta = 0.594534
             avgValidateDelta = 0.542366
             avgTestDelta = 0.542801
         superstep = 2
             avgTrainDelta = 0.322596
             avgValidateDelta = 0.373647
             avgTestDelta = 0.371556
         superstep = 3
             avgTrainDelta = 0.180468
             avgValidateDelta = 0.194503
             avgTestDelta = 0.198478
         superstep = 4
             avgTrainDelta = 0.113280
             avgValidateDelta = 0.117436
             avgTestDelta = 0.122555
         superstep = 5
             avgTrainDelta = 0.076510
             avgValidateDelta = 0.074419
             avgTestDelta = 0.077451
         superstep = 6
            avgTrainDelta = 0.051452
             avgValidateDelta = 0.051683
             avgTestDelta = 0.052538
         superstep = 7
             avgTrainDelta = 0.038257
             avgValidateDelta = 0.033629
             avgTestDelta = 0.034017
         superstep = 8
             avgTrainDelta = 0.027924
             avgValidateDelta = 0.026722
             avgTestDelta = 0.025877
         superstep = 9
             avgTrainDelta = 0.022886
             avgValidateDelta = 0.019267
             avgTestDelta = 0.018190
         superstep = 10
             avgTrainDelta = 0.018271
             avgValidateDelta = 0.015924
             avgTestDelta = 0.015377
    </skip>