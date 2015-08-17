Examples
--------
.. only:: html

    .. code::

        >>> a = ta.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ta.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ta.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ta.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ta.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR", max_iterations = 1, convergence_tolerance = 0.001)

.. only:: latex

    .. code::

        >>> a = ta.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ta.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ta.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ta.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ta.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR",
        ... max_iterations = 1, convergence_tolerance = 0.001)

The expected output is like this:

.. code::

    {'vertex_dictionary': {u'a': Frame "None"
    row_count = 29
    schema =
      _vid:int64
      _label:unicode
      node:int32
      PR:float64, u'b': Frame "None"
    row_count = 17437
    schema =
      _vid:int64
      _label:unicode
      node:int32
      PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
    row_count = 19265
    schema =
      _eid:int64
      _src_vid:int64
      _dest_vid:int64
      _label:unicode
      PR:float64, u'e2': Frame "None"
    row_count = 19265
    schema =
      _eid:int64
      _src_vid:int64
      _dest_vid:int64
      _label:unicode
      PR:float64}}

To query:

.. only:: html

    .. code::

        >>> a = ta.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ta.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ta.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ta.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ta.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR", max_iterations = 1, convergence_tolerance = 0.001)

        {'vertex_dictionary': {u'a': Frame "None"
        row_count = 29
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64, u'b': Frame "None"
        row_count = 17437
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64, u'e2': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64}}

.. only:: latex

    .. code::

        >>> a = ta.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ta.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ta.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ta.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ta.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR",
        ... max_iterations = 1, convergence_tolerance = 0.001)


        {'vertex_dictionary': {u'a': Frame "None"
        row_count = 29
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64, u'b': Frame "None"
        row_count = 17437
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64, u'e2': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64}}
