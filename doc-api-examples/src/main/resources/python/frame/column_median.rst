Examples
--------
Given a frame with column 'a' accessed by a Frame object 'my_frame':

.. code::

   >>> import trustedanalytics as ta
   >>> ta.connect()
   <connect>
   >>> data = [[2],[3],[3],[5],[7],[10],[30]]
   >>> schema = [('a', ta.int32)]
   >>> my_frame = ta.Frame(ta.UploadRows(data, schema))
   <progress>

Inspect my_frame

.. code::

   >>> my_frame.inspect()
   [#]  a
   =======
   [0]   2
   [1]   3
   [2]   3
   [3]   5
   [4]   7
   [5]  10
   [6]  30

Compute and return middle number of values in column *a*:

.. code::

   >>> median = my_frame.column_median('a')
   <progress>
   >>> print median
   5

Given a frame with column 'a' and column 'w' as weights accessed by a Frame object 'my_frame':

.. code::

   >>> data = [[2,1.7],[3,0.5],[3,1.2],[5,0.8],[7,1.1],[10,0.8],[30,0.1]]
   >>> schema = [('a', ta.int32), ('w', ta.float32)]
   >>> my_frame = ta.Frame(ta.UploadRows(data, schema))
   <progress>

Inspect my_frame

.. code::

   >>> my_frame.inspect()
   [#]  a   w
   =======================
   [0]   2   1.70000004768
   [1]   3             0.5
   [2]   3   1.20000004768
   [3]   5  0.800000011921
   [4]   7   1.10000002384
   [5]  10  0.800000011921
   [6]  30   0.10000000149


Compute and return middle number of values in column 'a' with weights 'w':

.. code::

   >>> median = my_frame.column_median('a', weights_column='w')
   <progress>
   >>> print median
   3
