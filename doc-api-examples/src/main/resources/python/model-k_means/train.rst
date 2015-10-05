Examples
--------
Train a KMeans Model

.. only:: html

    .. code::

        >>> my_model = ta.KMeansModel(name='MyKMeansModel')
        >>> my_model.train(train_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[1.0,2.0] 3, 10, 0.0002, "random")

.. only:: latex

    .. code::

        >>> my_model = ta.KMeansModel(name='MyKMeansModel')
        >>> my_model.train(train_frame, ['name_of_observation_column1',
        ... 'name_of_observation_column2'],[1.0,2.0] 3, 10, 0.0002, "random")
