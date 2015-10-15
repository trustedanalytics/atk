Examples
--------
Test the performance of a trained Svm Model

.. only:: html

    .. code::

        >>> my_model = ta.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',['List_of_observation_column/s'])

        >>> metrics['f_measure']
        0.66666666666666663

        >>> metrics['recall']
        0.5

        >>> metrics['accuracy']
        0.75

        >>> metrics['precision']
        1.0

        >>> metrics['confusion_matrix']
        {u'column_labels': [u'pos', u'neg'],
         u'matrix': [[2, 0], [0, 3]],
         u'row_labels': [u'pos', u'neg']}



.. only:: latex

    .. code::

        >>> my_model = ta.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])

        >>> metrics['f_measure']
        0.66666666666666663

        >>> metrics['recall']
        0.5

        >>> metrics['accuracy']
        0.75

        >>> metrics['precision']
        1.0

        >>> metrics['confusion_matrix']
        {u'column_labels': [u'pos', u'neg'],
         u'matrix': [[2, 0], [0, 3]],
         u'row_labels': [u'pos', u'neg']}