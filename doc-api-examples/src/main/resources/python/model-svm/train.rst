Examples
--------
.. only:: html

    .. code::

        >>> my_model = atk.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

.. only:: latex

    .. code::

        >>> my_model = atk.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, ['name_of_observation_column'],
        ... 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

