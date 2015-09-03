Examples
--------
::

    g.ml.latent_dirichlet_allocation(
            edge_value_property_list = "word_count",
            vertex_type_property_key = "vertex_type",
            input_edge_label_list = "contains",
            output_vertex_property_list = "lda_result ",
            vector_value = "true",
            num_topics = 3,
            max_supersteps=5
            )
     
An example output follows::

       {u'value': u'======Graph Statistics======
       Number of vertices: 12 (doc: 6, word: 6)
       Number of edges: 12

       ======|LDA| Configuration======
       numTopics: 3
       alpha: 0.100000
       beta: 0.100000
       convergenceThreshold: 0.000000
       bidirectionalCheck: false
       maxSupersteps: 5
       maxVal: Infinity
       minVal: -Infinity
       evaluateCost: false

       ======Learning Progress======
       superstep = 1    maxDelta = 0.333682
       superstep = 2    maxDelta = 0.117571

       superstep = 3    maxDelta = 0.073708
       superstep = 4    maxDelta = 0.053260
       superstep = 5    maxDelta = 0.038495
