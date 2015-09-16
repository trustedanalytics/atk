Examples
--------
Calculate the dot product for a sequence of columns in Frame object *my_frame*:

.. code::

     >>> my_frame.inspect()

       col_0:int32  col_1:float64  col_2:int32  col3:int32
     /-----------------------------------------------------/
       1            0.2            -2           5
       2            0.4            -1           6
       3            0.6             0           7
       4            0.8             1           8
       5            None            2           None

Modify the frame by computing the dot product for a sequence of columns:

.. code::

     >>> my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product')
     >>> my_frame.inspect()

       col_0:int32  col_1:float64 col_2:int32 col3:int32  dot_product:float64
     /------------------------------------------------------------------------/
       1            0.2           -2          5            -1.0
       2            0.4           -1          6             0.4
       3            0.6            0          7             4.2
       4            0.8            1          8            10.4
       5            None           2          None         10.0

Modify the frame by computing the dot product with default values for nulls:

.. only:: html

    .. code::

         >>> my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product_2', [0.1, 0.2], [0.3, 0.4])
         >>> my_frame.inspect()

           col_0:int32  col_1:float64 col_2:int32 col3:int32  dot_product:float64  dot_product_2:float64
         /--------------------------------------------------------------------------------------------/
            1            0.2           -2          5            -1.0               -1.0
            2            0.4           -1          6             0.4                0.4
            3            0.6            0          7             4.2                4.2
            4            0.8            1          8            10.4                10.4
            5            None           2          None         10.0                10.08

.. only:: latex

    .. code::

         >>> my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'],
         ... 'dot_product_2', [0.1, 0.2], [0.3, 0.4])
         >>> my_frame.inspect()

           col_0  col_1    col_2  col3   dot_product  dot_product_2
           int32  float64  int32  int32  float64      float64
         /----------------------------------------------------------/
            1     0.2      -2     5         -1.0         -1.0
            2     0.4      -1     6          0.4          0.4
            3     0.6       0     7          4.2          4.2
            4     0.8       1     8         10.4          10.4
            5     None      2     None      10.0          10.08

Calculate the dot product for columns of vectors in Frame object *my_frame*:

.. code::

     >>> my_frame.dot_product('col_4', 'col_5, 'dot_product')

     col_4:vector  col_5:vector  dot_product:float64
     /----------------------------------------------/
      [1, 0.2]     [-2, 5]       -1.0
      [2, 0.4]     [-1, 6]        0.4
      [3, 0.6]     [0,  7]        4.2
      [4, 0.8]     [1,  8]       10.4
