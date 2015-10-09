Examples
--------
Publish a trained Random Forest Classifier model for scoring

.. only:: html

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>

.. only:: latex

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>