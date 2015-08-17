Examples
--------
For this example, we calculate the top 5 movie genres in a data frame:

This example returns the top-3 rows sorted by a single column: 'year' descending:

.. code::

    >>> topk_frame = frame.sorted_k(3, [ ('year', False) ])

      col1:str   col2:int32   col3:str
    /-----------------------------------/
      Animation    2013       Frozen
      Animation    2010       Tangled
      Drama        2008       The Dark Knight


This example returns the top-5 rows sorted by multiple columns: 'genre' ascending and 'year' descending:

.. code::

    >>> topk_frame = frame.sorted_k(5, [ ('genre', True), ('year', False) ])

      col1:str   col2:int32   col3:str
    /-----------------------------------/
      Animation    2013       Frozen
      Animation    2010       Tangled
      Animation    1994       The Lion King
      Drama        2008       The Dark Knight
      Drama        1972       The Godfather

This example returns the top-5 rows sorted by multiple columns: 'genre' ascending and 'year' ascending
using the optional tuning parameter for reduce-tree depth:

.. code::

    >>> topk_frame = frame.sorted_k(5, [ ('genre', True), ('year', True) ], reduce_tree_depth=1)

      col1:str   col2:int32   col3:str
    /-----------------------------------/
      Animation    1994       The Lion King
      Animation    2010       Tangled
      Animation    2013       Frozen
      Drama        1972       The Godfather
      Drama        2008       The Dark Knight

