        """Documentation test for classifiers"""
        # Establish a connection to the ATK Rest Server
        # This handle will be used for the remaineder of the script
        # No cleanup is required

        # First you have to get your server URL and credentials file
        # from your TAP administrator
        atk_server_uri = os.getenv("ATK_SERVER_URI", ia.server.uri)
        credentials_file = os.getenv("ATK_CREDENTIALS", "")

        # set the server, and use the credentials to connect to the ATK
        ia.server.uri = atk_server_uri
        ia.connect(credentials_file)

        # Graphs are composed of 2 sets, one of nodes, and one of edges that
        # connect exactly two (possibly not distinct) nodes. The degree
        # of a node is the number of edges attached to it

        # Below we build a frame using a nodelist and an edgelist.


        node_frame = ia.Frame(
            ia.UploadRows([["node1"],
                           ["node2"],
                           ["node3"],
                           ["node4"],
                           ["node5"]],
                          [("node_name", str)]))
        edge_frame = ia.Frame(
            ia.UploadRows([["node2", "node3"],
                           ["node2", "node1"],
                           ["node2", "node4"],
                           ["node2", "node5"]],
                          [("from", str), ("to", str)]))

        # The graph is a center node on node2, with 4 nodes each attached to 
        # the center node. This is known as a star graph, in this configuration
        # it can be visualized as a plus sign

        # To Create a graph first you define the nodes, and then the edges

        graph = ia.Graph()

        graph.define_vertex_type("node")
        graph.define_edge_type("edge", "node", "node", directed=False)

        graph.vertices["node"].add_vertices(node_frame, "node_name")
        graph.edges["edge"].add_edges(edge_frame, "from", "to") 


        # get the degrees, which have known values
        degrees = graph.annotate_degrees("degree")


        degree_list = degrees["node"].take(5)
        known_list = [[4, u'node', u'node4', 1],
                      [1, u'node', u'node1', 1],
                      [5, u'node', u'node5', 1],
                      [2, u'node', u'node2', 4],
                      [3, u'node', u'node3', 1]]
        for i in known_list:
            self.assertIn(i, degree_list)


