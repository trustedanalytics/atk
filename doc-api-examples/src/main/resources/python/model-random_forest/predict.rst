Examples
--------
Predict using a Random Forest Model.
When predicting as a classifier a new column is added to the existing frame with the predicted class.
When predicting as a regressor a new column is added to the existing frame with the predicted value.

.. only:: html

    .. code::

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> predict_classifier_output = rf_classifier.predict(train_frame)
        >>> predict_classifier_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_class:int32
        -----------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1
            1            5.5548729596    2.7777687995                1
            0           46.1810010826    3.1611961917                0
            0           44.3117586448    3.3458963222                0
            0           34.6334526911    3.6429838715                0


        >>> rf_regressor = ta.RandomForestRegressorModel()
        >>> rf_regressor.train(train_frame, 'Class', ['Dim_1','Dim_2'])
        >>> predict_regressor_output = rf_regressor.predict(train_frame)
        >>> predict_regressor_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_value:float64
        -------------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1.0
            1            5.5548729596    2.7777687995                1.0
            0           46.1810010826    3.1611961917                0.0
            0           44.3117586448    3.3458963222                0.0
            0           34.6334526911    3.6429838715                0.0

.. only:: latex

    .. code::

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> predict_classifier_output = rf_classifier.predict(train_frame)
        >>> predict_classifier_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_class:int32
        -----------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1
            1            5.5548729596    2.7777687995                1
            0           46.1810010826    3.1611961917                0
            0           44.3117586448    3.3458963222                0
            0           34.6334526911    3.6429838715                0


        >>> rf_regressor = ta.RandomForestRegressorModel()
        >>> rf_regressor.train(train_frame, 'Class', ['Dim_1','Dim_2'])
        >>> predict_regressor_output = rf_regressor.predict(train_frame)
        >>> predict_regressor_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_value:float64
        -------------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1.0
            1            5.5548729596    2.7777687995                1.0
            0           46.1810010826    3.1611961917                0.0
            0           44.3117586448    3.3458963222                0.0
            0           34.6334526911    3.6429838715                0.0
