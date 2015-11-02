Examples
--------
Score using a Libsvm Model

.. only:: html

    .. code::

        >>> my_model = ta.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column1'])
        >>> predicted_label = my_model.score([-0.79798,   -0.0256669,    0.234375,   0.0140301,   -0.282051,    0.025012])

.. only:: latex

    .. code::

        >>> my_model = ta.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['name_of_observation_column1'])
        >>> predicted_label = my_model.score([-0.79798,   -0.0256669,    0.234375,   0.0140301,   -0.282051,    0.025012])

