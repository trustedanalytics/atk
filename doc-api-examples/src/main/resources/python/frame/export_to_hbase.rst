Examples
--------
    Overwrite/append scenarios (see below):

    1. create a simple hbase table from csv
       load csv into a frame using existing frame api
       save the frame into hbase (it creates a table - lets call it table1)

    2. overwrite existing table with new data
       do scenario 1 and create table1
       load the second csv into a frame
       save the frame into table1 (old data is gone)

    3. append data to the existing table 1
       do scenario 1 and create table1
       load table1 into frame1
       load csv into frame2
       let frame1 = frame1 + frame2 (concatenate frame2 into frame1)
       save frame1 into base as table1 (overwrite with initial + appended data)

.. code::

