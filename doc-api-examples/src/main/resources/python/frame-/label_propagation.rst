Examples
--------

    <skip>
    >>> s = [("a", ta.int32), ("b", ta.int32), ("c", ta.float32), ("d", ta.vector(2))]
    >>> rows = [ [1, 2, 0.5,"0.5,0.5"], [2, 3, 0.4, "-1,-1"], [3, 1, 0.1, "0.8,0.2"] ]
    >>> f = ta.Frame(ta.UploadRows (rows, s))
    <progress>

    >>> f.inspect()
    [#]    a   b   c       d
    =========================
    [0]    1   2  0.5 "0.5,0.5"
    [1]    2   3  0.4 "-1,-1"
    [2]    3   1  0.1 "0.8,0.2"

    >>> r = f.label_propagation("a", "b", "c", "d", "results")
    <progress>

    >>> r['frame'].inspect()
    [#]    features
    ===============
     [1]  "0.5,0.5"
     [2]  "0.6,0.4"
     [3]  "0.8,0.2"

    >>> r['report']
     ======Graph Statistics======
     Number of vertices: 600
     Number of edges: 15716

     ======LP Configuration======
     lambda: 0.000000
     anchorThreshold: 0.900000
     convergenceThreshold: 0.000000
     maxSupersteps: 10
     bidirectionalCheck: false

     ======Learning Progress======
     superstep = 1\tcost = 0.008692
     superstep = 2\tcost = 0.008155
     superstep = 3\tcost = 0.007809
     superstep = 4\tcost = 0.007544
     superstep = 5\tcost = 0.007328
     superstep = 6\tcost = 0.007142
     superstep = 7\tcost = 0.006979
     superstep = 8\tcost = 0.006833
     superstep = 9\tcost = 0.006701
     superstep = 10\tcost = 0.006580
    </skip>