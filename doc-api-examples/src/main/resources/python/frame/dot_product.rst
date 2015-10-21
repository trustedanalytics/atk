Examples
--------
<hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    <connect>

    >>> data = [[1, 0.2, -2, 5], [2, 0.4, -1, 6], [3, 0.6, 0, 7], [4, 0.8, 1, 8]]
    >>> schema = [('col_0', ta.int32), ('col_1', ta.float64),('col_2', ta.int32) ,('col_3', ta.int32)]

    >>> my_frame = ta.Frame(ta.UploadRows(data, schema))
    <progress>

</hide>
Calculate the dot product for a sequence of columns in Frame object *my_frame*:

.. code::

    >>> my_frame.inspect()
    [#]  col_0  col_1  col_2  col_3
    ===============================
    [0]      1    0.2     -2      5
    [1]      2    0.4     -1      6
    [2]      3    0.6      0      7
    [3]      4    0.8      1      8


Modify the frame by computing the dot product for a sequence of columns:

.. code::

     >>> my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product')
     <progress>

    >>> my_frame.inspect()
    [#]  col_0  col_1  col_2  col_3  dot_product
    ============================================
    [0]      1    0.2     -2      5         -1.0
    [1]      2    0.4     -1      6          0.4
    [2]      3    0.6      0      7          4.2
    [3]      4    0.8      1      8         10.4


Calculate the dot product for columns of vectors in Frame object *my_frame*:

<hide>
    >>> data = [[[1,0.2],[-2,5]],[[2,0.4],[-1,6]],[[3,0.6],[0,7]],[[4,0.8],[1,8]]]
    >>> schema = [('col_4', ta.vector(2)), ('col_5', ta.vector(2))]
    >>> my_frame = ta.Frame(ta.UploadRows(data, schema))
    <progress>

</hide>

.. code::
     >>> my_frame.dot_product('col_4', 'col_5', 'dot_product')
     <progress>

    >>> my_frame.inspect()
    [#]  col_4       col_5        dot_product
    =========================================
    [0]  [1.0, 0.2]  [-2.0, 5.0]         -1.0
    [1]  [2.0, 0.4]  [-1.0, 6.0]          0.4
    [2]  [3.0, 0.6]  [0.0, 7.0]           4.2
    [3]  [4.0, 0.8]  [1.0, 8.0]          10.4
