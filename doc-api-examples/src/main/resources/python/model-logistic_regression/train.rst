Examples
--------
Train a Logistic Regression Model using Limited-memory-BFGS.

In the example below, the flag for computing the covariance matrix is enabled.
When the covariance matrix is enabled, the summary table contains additional
statistics about the quality of the trained model.

.. only:: html

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> metrics = my_model.train(train_frame, 'name_of_label_column', ['obs1', 'obs2'], 'frequency_column', num_classes=2, optimizer='LBFGS', compute_covariance=True)

        >>> metrics.num_features
        2

        >>> metrics.num_classes
        2

        >>> metrics.summary_table

                    coefficients  degrees_freedom  standard_errors  wald_statistic   p_value
        intercept      0.924574                1         0.013052       70.836965    0.000000e+00
        obs1           0.405374                1         0.005793       69.973643    1.110223e-16
        obs2           0.707372                1         0.006709      105.439358    0.000000e+00

        >>> metrics.covariance_matrix.inspect()

        intercept:float64        obs1:float64        obs1:float64
        -------------------------------------------------------------
        0.000170358314027   1.85520616189e-05   3.60362435109e-05
        1.85520616189e-05   3.35615519594e-05   1.51513099821e-05
        3.60362435109e-05   1.51513099821e-05   4.50080801085e-05

.. only:: latex

    .. code::

        >>> my_model = ta.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame,'name_of_label_column', ['obs1', 'obs2'], 'frequency_column',
        ... num_classes=2, optimizer='LBFGS', compute_covariance=True)

        >>> metrics.num_features
        2

        >>> metrics.num_classes
        2

        >>> metrics.summary_table

                    coefficients  degrees_freedom  standard_errors  wald_statistic   p_value
        intercept      0.924574                1         0.013052       70.836965    0.000000e+00
        obs1           0.405374                1         0.005793       69.973643    1.110223e-16
        obs2           0.707372                1         0.006709      105.439358    0.000000e+00

        >>> metrics.covariance_matrix.inspect()

        intercept:float64        obs1:float64        obs1:float64
        -------------------------------------------------------------
        0.000170358314027   1.85520616189e-05   3.60362435109e-05
        1.85520616189e-05   3.35615519594e-05   1.51513099821e-05
        3.60362435109e-05   1.51513099821e-05   4.50080801085e-05
