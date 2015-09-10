Examples
--------
Train Random Forest Model as a classfier/regressor.

.. only:: html

    .. code::

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> train_classifier_output = rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)

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

        >>> rf_regressor = ta.RandomForestRegressorModel()
        >>> train_regressor_output = rf_regressor.train(train_frame, 'Class', ['Dim_1','Dim_2'])

        >>> train_regressor_output
        {u'feature_subset_category': u'all',
         u'impurity': u'variance',
         u'label_column': u'Class',
         u'max_bins': 100,
         u'max_depth': 4,
         u'num_nodes': 11,
         u'num_trees': 1,
         u'observation_columns': [u'Dim_1', u'Dim_2'],
         u'seed': 1622798860}

        >>> train_regressor_output['impurity']
        'variance'

.. only:: latex

    .. code::

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> train_classifier_output = rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)

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

        >>> rf_regressor = ta.RandomForestRegressorModel()
        >>> train_regressor_output = rf_regressor.train(train_frame, 'Class', ['Dim_1','Dim_2'])

        >>> train_regressor_output
        {u'feature_subset_category': u'all',
         u'impurity': u'variance',
         u'label_column': u'Class',
         u'max_bins': 100,
         u'max_depth': 4,
         u'num_nodes': 11,
         u'num_trees': 1,
         u'observation_columns': [u'Dim_1', u'Dim_2'],
         u'seed': 1622798860}

        >>> train_regressor_output['impurity']
        'variance'
