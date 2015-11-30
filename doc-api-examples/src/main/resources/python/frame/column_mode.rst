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
   

Compute and return a dictionary containing summary statistics of column *a*:

.. code::

   >>> mode = my_frame.column_mode('a')
   <progress>
   >>> print sorted(mode.items())
   [(u'mode_count', 1), (u'modes', [3]), (u'total_weight', 7.0), (u'weight_of_mode', 2.0)]

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
   

Compute and return dictionary containing summary statistics of column 'a' with weights 'w':

.. code::

   >>> mode = my_frame.column_mode('a', weights_column='w')
   <progress>
   >>> print sorted(mode.items())
   [(u'mode_count', 2), (u'modes', [2]), (u'total_weight', 6.200000144541264), (u'weight_of_mode', 1.7000000476837158)]

