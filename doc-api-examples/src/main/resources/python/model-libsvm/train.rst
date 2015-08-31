Examples
--------
.. only:: html

    .. code::

        >>> my_model = atk.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'], epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5, cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1, nr_weight=1, svm_type=2, kernel_type=2)

.. only:: latex

    .. code::

        >>> my_model = atk.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'],
        ... epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5,
        ... cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1,
        ... nr_weight=1, svm_type=2, kernel_type=2)

