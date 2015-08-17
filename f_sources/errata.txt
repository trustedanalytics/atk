======
Errata
======

.. index::
    single: column names

*   Frame column name can accept unicode characters, but it should be avoided
    because some functions such as delete_column will fail.

.. index::
    pair: characters; special

*   Renaming a graph to a name containing one or more of the special characters
    \@\#\$\%\^\&\* will cause the application to hang for a long time and then
    raise an error.

*   Attempting to create a frame with a parenthesis in the name will raise the
    error::

        trustedanalytics.rest.command.CommandServerError: Job aborted due to
        stage failure: Task 7.0:5 failed 4 times, most recent failure:
        Exception failure in TID 426 on host node03.zonda.cluster:
        java.lang.IllegalArgumentException: No enum constant parquet
        .schema.OriginalType.

*   Creating a table with an invalid source data file name causes the server to
    return an error message and abort, but also creates the empty (named)
    frame.

.. index::
    single: importing

*   When importing |CSV| data files to frames, small datasets may affect the
    number of lines skipped.
