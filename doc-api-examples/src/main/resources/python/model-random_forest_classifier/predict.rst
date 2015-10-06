Examples
--------
Predict using a Random Forest Classifier Model.

.. only:: html

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> predict_classifier_output = my_model.predict(train_frame)
        >>> predict_classifier_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_class:int32
        -----------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1
            1            5.5548729596    2.7777687995                1
            0           46.1810010826    3.1611961917                0
            0           44.3117586448    3.3458963222                0
            0           34.6334526911    3.6429838715                0

.. only:: latex

    .. code::
        >>> my_model = ta.RandomForestRegressorModel()
        >>> my_model.train(train_frame, 'Class', ['Dim_1','Dim_2'])
        >>> predict_regressor_output = my_model.predict(train_frame)
        >>> predict_regressor_output.inspect(5)
          Class:int32   Dim_1:float64   Dim_2:float64   predicted_value:float64
        -------------------------------------------------------------------------
            1           16.8973559126    2.6933495054                1.0
            1            5.5548729596    2.7777687995                1.0
            0           46.1810010826    3.1611961917                0.0
            0           44.3117586448    3.3458963222                0.0
            0           34.6334526911    3.6429838715                0.0