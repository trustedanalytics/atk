Examples
--------
Given a frame of coin flips, half heads and half tails, the entropy is simply ln(2):
.. code::

    >>> print frame.inspect()

              data:unicode  
            /----------------/
              H             
              T             
              H             
              T             
              H             
              T             
              H             
              T             
              H             
              T             

    >>> print "Computed entropy:", frame.entropy("data")

            Computed entropy: 0.69314718056

If we have more choices and weights, the computation is not as simple.
An on-line search for "Shannon Entropy" will provide more detail.

.. code::

   >>> print frame.inspect()
              data:int32   weight:int32  
            -----------------------------
                       0              1  
                       1              2  
                       2              4  
                       4              8  

   >>> print "Computed entropy:", frame.entropy("data", "weight")

            Computed entropy: 1.13691659183

