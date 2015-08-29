Examples
--------
.. only:: html

    .. code::

        >>> my_model = atk.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',['List_of_observation_column/s'])

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

        >>> my_model = atk.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])

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


