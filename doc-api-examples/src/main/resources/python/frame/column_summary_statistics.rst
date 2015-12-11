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

Compute and return summary statistics for values in column *a*:

.. code::

   >>> summary_statistics = my_frame.column_summary_statistics('a')
   <progress>
   >>> print sorted(summary_statistics.items())
   [(u'bad_row_count', 0), (u'geometric_mean', 5.6725751451901045), (u'good_row_count', 7), (u'maximum', 30.0), (u'mean', 8.571428571428571), (u'mean_confidence_lower', 1.277083729932067), (u'mean_confidence_upper', 15.865773412925076), (u'minimum', 2.0), (u'non_positive_weight_count', 0), (u'positive_weight_count', 7), (u'standard_deviation', 9.846440014156434), (u'total_weight', 7.0), (u'variance', 96.95238095238095)]

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


Compute and return summary statistics values in column 'a' with weights 'w':

.. code::
   >>> summary_statistics = my_frame.column_summary_statistics('a', weights_column='w')
   <progress>
   >>> print sorted(summary_statistics.items())
   [(u'bad_row_count', 0), (u'geometric_mean', 4.039682869616821), (u'good_row_count', 7), (u'maximum', 30.0), (u'mean', 5.032258048622591), (u'mean_confidence_lower', 1.4284724667085964), (u'mean_confidence_upper', 8.636043630536586), (u'minimum', 2.0), (u'non_positive_weight_count', 0), (u'positive_weight_count', 7), (u'standard_deviation', 4.578241754132706), (u'total_weight', 6.200000144541264), (u'variance', 20.96029755928412)]
