Examples
--------
<hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    <connect>

    >>> data = [[100],[250],[95],[179],[315],[660],[540],[420],[250],[335]]
    >>> schema = [('final_sale_price', ta.int32)]

    >>> my_frame = ta.Frame(ta.UploadRows(data, schema))
    <progress>

</hide>
Consider Frame *my_frame*, which accesses a frame that contains a single
column *final_sale_price*:

.. code::

    >>> my_frame.inspect()
    [#]  final_sale_price
    =====================
    [0]               100
    [1]               250
    [2]                95
    [3]               179
    [4]               315
    [5]               660
    [6]               540
    [7]               420
    [8]               250
    [9]               335

To calculate 10th, 50th, and 100th quantile:

.. code::

    >>> quantiles_frame = my_frame.quantiles('final_sale_price', [10, 50, 100])
    <progress>

A new Frame containing the requested Quantiles and their respective values
will be returned :

.. code::

   >>> quantiles_frame.inspect()
   [#]  Quantiles  final_sale_price_QuantileValue
   ==============================================
   [0]       10.0                            95.0
   [1]       50.0                           250.0
   [2]      100.0                           660.0
