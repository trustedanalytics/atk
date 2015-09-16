Examples
--------

.. only:: html

    .. code::

        >>> my_model = ta.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column1'])
        >>> predicted_frame = my_model.predict(predict_frame, ['predict_for_observation_column'])

.. only:: latex

    .. code::

        >>> my_model = ta.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['name_of_observation_column1'])
        >>> predicted_frame = my_model.predict(predict_frame,
        ... ['predict_for_observation_column'])

