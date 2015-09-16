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

my_frame.correlation computes the common correlation coefficient (Pearson's) on the pair
of columns provided.
In this example, the idnum and most of the columns have trivial correlations: -1, 0, or +1.
Column x3 provides a contrasting coefficient of 3 / sqrt(3) = 0.948683298051 .

.. code::

    >>> my_frame.correlation(["x1", "x2"])
       -1.0
    >>> my_frame.correlation(["x1", "x4"])
        0.0
    >>> my_frame.correlation(["x2", "x3"])
        -0.948683298051
