Examples
--------
Start with a frame with columns *Black* and *White*.


    <hide>

    >>> import trustedanalytics as ta
    >>> ta.connect()

    -etc-

    >>> s = [('Black', str), ('White', str)]

    >>> rows = [["glass", "clear"],["paper","unclear"]]

    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))

    -etc-

    </hide>

    >>> print my_frame.schema
    [(u'Black', <type 'unicode'>), (u'White', <type 'unicode'>)]

Rename the columns to *Mercury* and *Venus*:

    >>> my_frame.rename_columns({"Black": "Mercury", "White": "Venus"})

    >>> print my_frame.schema
    [(u'Mercury', <type 'unicode'>), (u'Venus', <type 'unicode'>)]

