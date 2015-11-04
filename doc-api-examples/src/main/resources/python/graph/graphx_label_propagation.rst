<skip>
>>> import trustedanalytics as ta
>>> ta.connect()
>>> s = [("a", ta.int32), ("b", ta.int32), ("c", ta.float32)]
>>> rows = [ [1, 2, 0.5], [2, 3, 0.4], [3, 1, 0.1] ]
>>> frame = ta.Frame(ta.UploadRows (rows, s))
<progress>

>>> f.inspect()
[#]    a   b  c
=================
[0]    1   2  0.5
[1]    2   3  0.4
[2]    3   1  0.1

>>> graph = ta.Graph()
>>> graph.define_vertex_type("node")
>>> graph.vertices["node"].add_vertices(frame,"a")
>>> graph.vertices["node"].add_vertices(frame,"b")
>>> graph.define_edge_type("c","a","b",directed=True)
>>> graph.edges["c"].add_edges(frame,"a","b")
>>> f= graph.graphx_label_propagation(output_vertex_property_name = "propagatedLabel")

>>> f.inspect()
[#]    propagatedLabel
======================
 [1]   [0.5]
 [2]   [0.6]
 [3]   [0.2]

</skip>
