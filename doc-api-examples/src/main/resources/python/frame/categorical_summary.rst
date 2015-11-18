Examples
--------
Consider Frame *my_frame*, which contains the data

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [("source",str),("target",str)]
    >>> rows = [ ["entity","thing"], ["entity","physical_entity"],["entity","abstraction"],["physical_entity","entity"],["physical_entity","matter"],["physical_entity","process"],["physical_entity","thing"],["physical_entity","substance"],["physical_entity","object"],["physical_entity","causal_agent"],["abstraction","entity"],["abstraction","communication"],["abstraction","group"],["abstraction","otherworld"],["abstraction","psychological_feature"],["abstraction","attribute"],["abstraction","set"],["abstraction","measure"],["abstraction","relation"],["thing","physical_entity"],["thing","reservoir"],["thing","part"],["thing","subject"],["thing","necessity"],["thing","variable"],["thing","unit"],["thing","inessential"],["thing","body_of_water"]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
    [#]  source           target
    =====================================
    [0]  entity           thing
    [1]  entity           physical_entity
    [2]  entity           abstraction
    [3]  physical_entity  entity
    [4]  physical_entity  matter
    [5]  physical_entity  process
    [6]  physical_entity  thing
    [7]  physical_entity  substance
    [8]  physical_entity  object
    [9]  physical_entity  causal_agent

    >>> cm = my_frame.categorical_summary(('source', {'top_k' : 2}))
    <progress>

    >>> cm
    {u'categorical_summary': [{u'column': u'source', u'levels': [{u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'}, {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 0.35714285714285715, u'frequency': 10, u'level': u'Other'}]}]}

    >>> cm = my_frame.categorical_summary(('source', {'threshold' : 0.5}))
    <progress>

    >>> cm
    {u'categorical_summary': [{u'column': u'source', u'levels': [{u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 1.0, u'frequency': 28, u'level': u'Other'}]}]}

    >>> cm = my_frame.categorical_summary(('source', {'top_k' : 2}), ('target', {'threshold' : 0.5}))
    <progress>

    >>> cm
    {u'categorical_summary': [{u'column': u'source', u'levels': [{u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'}, {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'}, {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 0.35714285714285715, u'frequency': 10, u'level': u'Other'}]}, {u'column': u'target', u'levels': [{u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'}, {u'percentage': 1.0, u'frequency': 28, u'level': u'Other'}]}]}







