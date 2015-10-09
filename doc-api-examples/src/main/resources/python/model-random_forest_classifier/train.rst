Examples
--------
Train Random Forest Model as a classifier.

.. only:: html

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> train_classifier_output = my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)

        >>> train_classifier_output
        {u'feature_subset_category': u'all',
         u'impurity': u'gini',
         u'label_column': u'Class',
         u'max_bins': 100,
         u'max_depth': 4,
         u'num_classes': 2,
         u'num_nodes': 11,
         u'num_trees': 1,
         u'observation_columns': [u'Dim_1', u'Dim_2'],
         u'seed': -579418825}

        >>> train_classifier_output['num_nodes']
        11

.. only:: latex

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> train_classifier_output = my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)

        >>> train_classifier_output
        {u'feature_subset_category': u'all',
         u'impurity': u'gini',
         u'label_column': u'Class',
         u'max_bins': 100,
         u'max_depth': 4,
         u'num_classes': 2,
         u'num_nodes': 11,
         u'num_trees': 1,
         u'observation_columns': [u'Dim_1', u'Dim_2'],
         u'seed': -579418825}

        >>> train_classifier_output['num_nodes']
        11
