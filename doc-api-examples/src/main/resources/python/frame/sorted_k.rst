Examples
--------
These examples deal with the most recently-released movies in a private collection.
Consider the movie collection already stored in the frame below:

.. code::

    >>> big_frame.inspect(10)

      genre:str  year:int32   title:str
    /-----------------------------------/
      Drama        1957       12 Angry Men
      Crime        1946       The Big Sleep
      Western      1969       Butch Cassidy and the Sundance Kid
      Drama        1971       A Clockwork Orange
      Drama        2008       The Dark Knight
      Animation    2013       Frozen
      Drama        1972       The Godfather
      Animation    1994       The Lion King
      Animation    2010       Tangled
      Fantasy      1939       The Wonderful Wizard of Oz


This example returns the top 3 rows sorted by a single column: 'year' descending:

.. code::

    >>> topk_frame = big_frame.sorted_k(3, [ ('year', False) ])
    >>> topk_frame.inspect()

      genre:str  year:int32   title:str
    /-----------------------------------/
      Animation    2013       Frozen
      Animation    2010       Tangled
      Drama        2008       The Dark Knight


This example returns the top 5 rows sorted by multiple columns: 'genre' ascending, then 'year' descending:

.. code::

    >>> topk_frame = big_frame.sorted_k(5, [ ('genre', True), ('year', False) ])
    >>> topk_frame.inspect()

      genre:str  year:int32   title:str
    /-----------------------------------/
      Animation    2013       Frozen
      Animation    2010       Tangled
      Animation    1994       The Lion King
      Crime        1946       The Big Sleep
      Drama        2008       The Dark Knight

This example returns the top 5 rows sorted by multiple columns: 'genre'
ascending, then 'year' ascending.
It also illustrates the optional tuning parameter for reduce-tree depth
(which does not affect the final result).

.. code::

    >>> topk_frame = big_frame.sorted_k(5, [ ('genre', True), ('year', True) ], reduce_tree_depth=1)
    >>> topk_frame.inspect()

      genre:str  year:int32   title:str
    /-----------------------------------/
      Animation    1994       The Lion King
      Animation    2010       Tangled
      Animation    2013       Frozen
      Crime        1946       The Big Sleep
      Drama        1972       The Godfather

