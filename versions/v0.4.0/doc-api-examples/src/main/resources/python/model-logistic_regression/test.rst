Examples
--------
.. only:: html

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
        >>> metrics = my_model.test(test_frame, 'name_of_label_column','name_of_observation_column')

        >>> metrics.f_measure
        0.66666666666666663

        >>> metrics.recall
        0.5

        >>> metrics.accuracy
        0.75

        >>> metrics.precision
        1.0

        >>> metrics.confusion_matrix

                      Predicted
                    _pos_ _neg__
        Actual  pos |  1     1
                neg |  0     2

.. only:: latex

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column',
        ... 'name_of_label_column')
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',
        ... 'name_of_observation_column')

        >>> metrics.f_measure
        0.66666666666666663

        >>> metrics.recall
        0.5

        >>> metrics.accuracy
        0.75

        >>> metrics.precision
        1.0

        >>> metrics.confusion_matrix

                      Predicted
                    _pos_ _neg__
        Actual  pos |  1     1
                neg |  0     2


