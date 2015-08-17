Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column *final_sale_price*:

.. code::

    >>> my_frame.inspect()

      final_sale_price:int32
    /------------------------/
                100
                250
                 95
                179
                315
                660
                540
                420
                250
                335

To calculate 10th, 50th, and 100th quantile:

.. code::

    >>> quantiles_frame = my_frame.quantiles('final_sale_price', [10, 50, 100])

A new Frame containing the requested Quantiles and their respective values
will be returned :

.. code::

   >>> quantiles_frame.inspect()

     Quantiles:float64   final_sale_price_QuantileValue:float64
   /------------------------------------------------------------/
            10.0                                     95.0
            50.0                                    250.0
           100.0                                    660.0


