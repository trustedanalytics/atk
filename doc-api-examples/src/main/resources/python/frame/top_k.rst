Examples
--------
For this example, we calculate the top 5 movie genres in a data frame:

.. code::

    >>> top5 = frame.top_k('genre', 5)
    >>> top5.inspect()

      genre:str   count:float64
    /---------------------------/
      Drama        738278
      Comedy       671398
      Short        455728
      Documentary  323150
      Talk-Show    265180

This example calculates the top 3 movies weighted by rating:

.. code::

    >>> top3 = frame.top_k('genre', 3, weights_column='rating')
    >>> top3.inspect()

      movie:str      count:float64
    /------------------------------/
      The Godfather         7689.0
      Shawshank Redemption  6358.0
      The Dark Knight       5426.0

This example calculates the bottom 3 movie genres in a data frame:

.. code::

    >>> bottom3 = frame.top_k('genre', -3)
    >>> bottom3.inspect()

      genre:str   count:float64
    /---------------------------/
      Musical       26
      War           47
      Film-Noir    595


