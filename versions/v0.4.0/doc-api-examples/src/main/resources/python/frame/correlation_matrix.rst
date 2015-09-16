Examples
--------
Consider Frame *my_frame*, which contains the data

.. code::

    >>> my_frame.inspect()

     idnum:int32   x1:float32   x2:float32   x3:float32   x4:float32  
   /-------------------------------------------------------------------/
               0          1.0          4.0          0.0         -1.0  
               1          2.0          3.0          0.0         -1.0  
               2          3.0          2.0          1.0         -1.0  
               3          4.0          1.0          2.0         -1.0  
               4          5.0          0.0          2.0         -1.0  

my_frame.correlation_matrix computes the common correlation coefficient (Pearson's) on each pair
of columns in the user-provided list.
In this example, the idnum and most of the columns have trivial correlations: -1, 0, or +1.
Column x3 provides a contrasting coefficient of 3 / sqrt(3) = 0.948683298051 .
The resulting table (specifying all columns) is

.. code::

    >>> corr_matrix = my_frame.correlation_matrix(my_frame.column_names)
    >>> corr_matrix.inspect()

      idnum:float64       x1:float64        x2:float64        x3:float64   x4:float64  
   ------------------------------------------------------------------------------------
                1.0              1.0              -1.0    0.948683298051          0.0  
                1.0              1.0              -1.0    0.948683298051          0.0  
               -1.0             -1.0               1.0   -0.948683298051          0.0  
     0.948683298051   0.948683298051   -0.948683298051               1.0          0.0  
                0.0              0.0               0.0               0.0          1.0  

