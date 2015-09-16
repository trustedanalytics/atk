Examples
--------
Start with a frame with columns *Wrong* and *Wong*.

.. code::

    >>> print my_frame.schema
    [('Wrong', str), ('Wong', str)]

Rename the columns to *Right* and *Wite*:

.. code::

    >>> my_frame.rename_columns({"Wrong": "Right, "Wong": "Wite"})

Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

.. code::

    >>> print my_frame.schema
    [('Right', str), ('Wite', str)]
