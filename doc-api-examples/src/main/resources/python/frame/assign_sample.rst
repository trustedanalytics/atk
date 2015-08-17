Examples
--------
Given a frame accessed via Frame *my_frame*:

.. code::

    >>> my_frame.inspect()
      col_nc:str  col_wk:str
    /------------------------/
      abc         zzz
      def         yyy
      ghi         xxx
      jkl         www
      mno         vvv
      pqr         uuu
      stu         ttt
      vwx         sss
      yza         rrr
      bcd         qqq

To append a new column *sample_bin* to the frame and assign the value in the
new column to "train", "test", or "validate":

.. code::

    >>> my_frame.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])
    >>> my_frame.inspect()
      col_nc:str  col_wk:str  sample_bin:str
    /----------------------------------------/
      abc         zzz         validate
      def         yyy         test
      ghi         xxx         test
      jkl         www         test
      mno         vvv         train
      pqr         uuu         validate
      stu         ttt         validate
      vwx         sss         train
      yza         rrr         validate
      bcd         qqq         train

Now, the frame accessed by the Frame, *my_frame*, has a new column named
"sample_bin" and each row contains one of the values "train", "test", or
"validate".
Values in the other columns are unaffected.

