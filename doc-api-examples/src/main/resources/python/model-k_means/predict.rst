Examples
--------
.. only:: html

    .. code::

        >>> my_model = atk.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> new_frame = my_model.predict(my_frame)

.. only:: latex

    .. code::

        >>> my_model = atk.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1',
        ... 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> new_frame = my_model.predict(my_frame)


