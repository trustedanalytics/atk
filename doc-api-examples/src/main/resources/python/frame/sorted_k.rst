Examples
--------
These examples deal with the most recently-released movies in a private collection.
Consider the movie collection already stored in the frame below:

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [("genre", str), ("year", ta.int32), ("title", str)]
    >>> rows = [["Drama", 1957, "12 Angry Men"], ["Crime", 1946, "The Big Sleep"], ["Western", 1969, "Butch Cassidy and the Sundance Kid"], ["Drama", 1971, "A Clockwork Orange"], ["Drama", 2008, "The Dark Knight"], ["Animation", 2013, "Frozen"], ["Drama", 1972, "The Godfather"], ["Animation", 1994, "The Lion King"], ["Animation", 2010, "Tangled"], ["Fantasy", 1939, "The WOnderful Wizard of Oz"]  ]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
    [#]  genre      year  title
    ========================================================
    [0]  Drama      1957  12 Angry Men
    [1]  Crime      1946  The Big Sleep
    [2]  Western    1969  Butch Cassidy and the Sundance Kid
    [3]  Drama      1971  A Clockwork Orange
    [4]  Drama      2008  The Dark Knight
    [5]  Animation  2013  Frozen
    [6]  Drama      1972  The Godfather
    [7]  Animation  1994  The Lion King
    [8]  Animation  2010  Tangled
    [9]  Fantasy    1939  The WOnderful Wizard of Oz


This example returns the top 3 rows sorted by a single column: 'year' descending:

    >>> topk_frame = my_frame.sorted_k(3, [ ('year', False) ])
    <progress>

    >>> topk_frame.inspect()
    [#]  genre      year  title
    =====================================
    [0]  Animation  2013  Frozen
    [1]  Animation  2010  Tangled
    [2]  Drama      2008  The Dark Knight

This example returns the top 5 rows sorted by multiple columns: 'genre' ascending, then 'year' descending:

    >>> topk_frame = my_frame.sorted_k(5, [ ('genre', True), ('year', False) ])
    <progress>

    >>> topk_frame.inspect()
    [#]  genre      year  title
    =====================================
    [0]  Animation  2013  Frozen
    [1]  Animation  2010  Tangled
    [2]  Animation  1994  The Lion King
    [3]  Crime      1946  The Big Sleep
    [4]  Drama      2008  The Dark Knight


This example returns the top 5 rows sorted by multiple columns: 'genre'
ascending, then 'year' ascending.
It also illustrates the optional tuning parameter for reduce-tree depth
(which does not affect the final result).

    >>> topk_frame = my_frame.sorted_k(5, [ ('genre', True), ('year', True) ], reduce_tree_depth=1)
    <progress>

    >>> topk_frame.inspect()
    [#]  genre      year  title
    ===================================
    [0]  Animation  1994  The Lion King
    [1]  Animation  2010  Tangled
    [2]  Animation  2013  Frozen
    [3]  Crime      1946  The Big Sleep
    [4]  Drama      1957  12 Angry Men


