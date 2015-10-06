Examples
--------
Predict using a Logistic Regression Model

.. only:: html

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column', 'name_of_label_column', num_classes=2, optimizer="LBFGS"))
        >>> my_model.predict(predict_frame, ['predict_for_observation_column'])

.. only:: latex

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column',
        ... 'name_of_label_column', num_classes=2, optimizer="LBFGS"))
        >>> my_model.predict(predict_frame, ['predict_for_observation_column'])

