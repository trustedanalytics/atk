Examples
--------
Predict using a Naive Bayes Model

.. only:: html

    .. code::

        >>> my_model = ta.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'])
        >>> output = my_model.predict(predict_frame, ['name_of_observation_column(s)'])
        >>> output.inspect(5)

              Class:int32   Dim_1:int32   Dim_2:int32   Dim_3:int32   predicted_class:float64
            -----------------------------------------------------------------------------------
                        0             1             0             0                       0.0
                        1             0             1             0                       1.0
                        1             0             2             0                       1.0
                        2             0             0             1                       2.0
                        2             0             0             2                       2.0

.. only:: latex

    .. code::

        >>> my_model = ta.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column', ['name_of_observation_column(s)'])
        >>> output = my_model.predict(predict_frame, ['name_of_observation_column(s)'])
        >>> output.inspect(5)

              Class:int32   Dim_1:int32   Dim_2:int32   Dim_3:int32   predicted_class:float64
            -----------------------------------------------------------------------------------
                        0             1             0             0                       0.0
                        1             0             1             0                       1.0
                        1             0             2             0                       1.0
                        2             0             0             1                       2.0
                        2             0             0             2                       2.0

