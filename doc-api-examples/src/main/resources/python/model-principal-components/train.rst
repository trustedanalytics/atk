Examples
--------

.. only:: html

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> train_output = my_model.train(train_frame,["1","2","3"],k=3)
        >>> train_output
        {u'column_means': [3.0, 1.5699999999999998, 0.30000000000000004],
         u'k': 3,
         u'mean_centered': True,
         u'observation_columns': [u'1', u'2', u'3'],
         u'right_singular_vectors': [[-0.9880604079662845, 0.1524554460423363,0.022225372472291664],
           [-0.1475177133741632, -0.9777763055278146, 0.1489698647016758],
           [0.044442709754897426, 0.14391258916581845, 0.9885919341311823]],
         u'singular_values': [1.9528500871335064,1.258951004642986,0.3498841309506238]}
        >>> right_singular_vectors = output['right_singular_vectors']
        >>> right_singular_vectors[0]
         [-0.9880604079662845, 0.1524554460423363, 0.022225372472291664]

.. only:: latex

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> train_output = my_model.train(train_frame,["1","2","3"],k=3)
        >>> train_output
        {u'column_means': [3.0, 1.5699999999999998, 0.30000000000000004],
         u'k': 3,
         u'mean_centered': True,
         u'observation_columns': [u'1', u'2', u'3'],
         u'right_singular_vectors': [[-0.9880604079662845, 0.1524554460423363,0.022225372472291664],
           [-0.1475177133741632, -0.9777763055278146, 0.1489698647016758],
           [0.044442709754897426, 0.14391258916581845, 0.9885919341311823]],
         u'singular_values': [1.9528500871335064,1.258951004642986,0.3498841309506238]}
        >>> right_singular_vectors = output['right_singular_vectors']
        >>> right_singular_vectors[0]
         [-0.9880604079662845, 0.1524554460423363, 0.022225372472291664]



