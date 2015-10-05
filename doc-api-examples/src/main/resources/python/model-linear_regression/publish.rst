Examples
--------
Publish a trained Linear Regression Model for scoring

.. only:: html

    .. code::

        >>> my_model = ta.LinearRegressionModel(name='LinReg')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'],false, 50, 1.0, "L1", 0.02, 1.0)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>

.. only:: latex

    .. code::

        >>> my_model = ta.LinearRegressionModel(name='LinReg')
        >>> my_model.train(train_frame, 'name_of_label_column', ['name_of_observation_column(s)'],
        ...  false, 50, 1.0, "L1", 0.02, 1.0)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>
