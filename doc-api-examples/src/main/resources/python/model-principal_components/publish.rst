Examples
--------
Publish a trained Principal Components Model for scoring

.. only:: html

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model.train(train_frame,["1","2","3"],k=3)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>

.. only:: latex

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model.train(train_frame,["1","2","3"],k=3)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>