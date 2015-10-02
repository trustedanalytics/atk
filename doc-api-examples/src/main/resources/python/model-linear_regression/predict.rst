Examples
--------
Predict using a Linear Regression Model

.. only:: html

    .. code::

        >>> my_model = ta.LinearRegressionModel(name='LinReg')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'])
        >>> my_model.predict(predict_frame, ['name_of_observation_column(s)'])

.. only:: latex

    .. code::

        >>> my_model = ta.LinearRegressionModel(name='LinReg')
        >>> my_model.train(train_frame, 'name_of_label_column', ['name_of_observation_column(s)'])
        >>> my_model.predict(predict_frame, ['name_of_observation_column(s)'])

