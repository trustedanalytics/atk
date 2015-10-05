Examples
--------
Publish a trained KMeans Model for scoring

.. only:: html

    .. code::

        >>> my_model = ta.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> my_model.publish()
        <Path in HDFS to model's tar file>

.. only:: latex

    .. code::

        >>> my_model = ta.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> my_model.publish()
        <Path in HDFS to model's tar file>


