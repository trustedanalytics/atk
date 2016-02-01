.. _ds_graph:

======
Graphs
======

        # Graphs are composed of 2 sets, one of vertices, and one of edges that
        # connect exactly two (possibly not distinct) vertices. The degree
        # of a vertex is the number of edges attached to it

Setup
-----

Establish a connection to the ATK Rest Server.
This handle will be used for the remainder of the script.

Get server URL and credentials file from the TAP administrator.

.. code::

   atk_server_uri = os.getenv("ATK_SERVER_URI", ia.server.uri)
   credentials_file = os.getenv("ATK_CREDENTIALS", "")

Set the server, and use the credentials to connect to the ATK.

.. code::

   ia.server.uri = atk_server_uri
   ia.connect(credentials_file)

Build a Graph
-------------

Below we build a frame using a vertexlist and an edgelist.

.. code::

   vertex_frame = ia.Frame(
       ia.UploadRows([["vertex1"],
                      ["vertex2"],
                      ["vertex3"],
                      ["vertex4"],
                      ["vertex5"]],
                     [("vertex_name", str)]))
   edge_frame = ia.Frame(
       ia.UploadRows([["vertex2", "vertex3"],
                      ["vertex2", "vertex1"],
                      ["vertex2", "vertex4"],
                      ["vertex2", "vertex5"]],
                     [("from", str), ("to", str)]))

The graph is a center vertex (vertex20, with four vertices each attached to the center vertex. This is known as a star graph. It can be visualized as a plus sign.

To Create a graph define the vertices, then define the edges.

.. code::
   graph = ia.Graph()

   graph.define_vertex_type("vertex")
   graph.define_edge_type("edge", "vertex", "vertex", directed=False)

   graph.vertices["vertex"].add_vertices(vertex_frame, "vertex_name")
   graph.edges["edge"].add_edges(edge_frame, "from", "to") 

The degree of a vertex is how many edges are connected to the vertex.

.. code::

   degrees = graph.annotate_degrees("degree")


   degree_list = degrees["vertex"].take(5)
   known_list = [[4, u'vertex', u'vertex4', 1],
                 [1, u'vertex', u'vertex1', 1],
                 [5, u'vertex', u'vertex5', 1],
                 [2, u'vertex', u'vertex2', 4],
                 [3, u'vertex', u'vertex3', 1]]

   for i in known_list:
       self.assertIn(i, degree_list)


