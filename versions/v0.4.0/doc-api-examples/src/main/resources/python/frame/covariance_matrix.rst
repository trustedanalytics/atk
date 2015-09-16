Examples
--------
Consider Frame *my_frame1*, which computes the covariance matrix for three
numeric columns:

.. code::

    >>> my_frame1.inspect()

      col_0:int64    col_1:int64   col_3:float64
    \--------------------------------------------\
        1            4             33.4
        2            5             43.7
        3            6             20.1

    >>> cov_matrix = my_frame1.covariance_matrix(['col_0', 'col_1', 'col_2'])
    >>> cov_matrix.inspect()

      col_0:float64    col_1:float64   col_3:float64
    \------------------------------------------------\
         1.00             1.00            -6.65
         1.00             1.00            -6.65
         -6.65           -6.65            139.99

Consider Frame *my_frame2*, which computes the covariance matrix for a single
vector column:

.. code::

    >>> my_frame2.inspect()

      State:unicode             Population_HISTOGRAM:vector
    \-------------------------------------------------------\
        Louisiana               [0.0, 1.0, 0.0, 0.0]
        Georgia                 [0.0, 1.0, 0.0, 0.0]
        Texas                   [0.0, 0.54, 0.46, 0.0]
        Florida                 [0.0, 0.83, 0.17, 0.0]

    >>> cov_matrix = my_frame2.covariance_matrix(['Population_HISTOGRAM'])
    >>> cov_matrix.inspect()

      Population_HISTOGRAM:vector
    \-------------------------------------\
      [0,  0.00000000,  0.00000000,    0]
      [0,  0.04709167, -0.04709167,    0]
      [0, -0.04709167,  0.04709167,    0]
      [0,  0.00000000,  0.00000000,    0]


