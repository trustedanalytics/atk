Examples
--------
Train a Naive Bayes Model

.. only:: html

    .. code::

        >>> my_model = ta.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'],0.9)

.. only:: latex

    .. code::

        >>> my_model = ta.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'],0.9)

