Examples
--------

.. code::

    >>> frame.categorical_summary('source','target')
    >>> frame.categorical_summary(('source', {'top_k' : 2}))
    >>> frame.categorical_summary(('source', {'threshold' : 0.5}))
    >>> frame.categorical_summary(('source', {'top_k' : 2}), ('target',
    ... {'threshold' : 0.5}))

.. only:: html

    Sample output::

        {u'categorical_summary': [{u'column': u'source', u'levels': [{u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'}, {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'}, {u'percentage': 0.25, u'frequency': 7, u'level': u'physical_entity'}, {u'percentage': 0.10714285714285714, u'frequency': 3, u'level': u'entity'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Other'}]}, {u'column': u'target', u'levels': [ {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'thing'}, {u'percentage': 0.07142857142857142, u'frequency': 2,  u'level': u'physical_entity'}, {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'entity'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'variable'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'unit'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'substance'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'subject'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'set'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'reservoir'}, {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'relation'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 0.5357142857142857, u'frequency': 15, u'level': u'Other'}]}]}

.. only:: latex

    Sample output::

        {u'categorical_summary': [{u'column': u'source', u'levels': [
        {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'},
        {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'},
        {u'percentage': 0.25, u'frequency': 7, u'level': u'physical_entity'},
        {u'percentage': 0.10714285714285714, u'frequency': 3, u'level': u'entity'},
        {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
        {u'percentage': 0.0, u'frequency': 0, u'level': u'Other'}]},
        {u'column': u'target', u'levels': [
        {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'thing'},
        {u'percentage': 0.07142857142857142, u'frequency': 2,
         u'level': u'physical_entity'},
        {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'entity'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'variable'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'unit'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'substance'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'subject'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'set'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'reservoir'},
        {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'relation'},
        {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
        {u'percentage': 0.5357142857142857, u'frequency': 15, u'level': u'Other'}]}]}

