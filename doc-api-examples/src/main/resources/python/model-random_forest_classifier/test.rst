Examples
--------
Test the performance of a trained Random Forest Classifier Model

.. only:: html

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> my_model.test(train_frame,'Class')
          Precision: 1.0
          Recall: 1.0
          Accuracy: 1.0
          FMeasure: 1.0
          Confusion Matrix:
                        Predicted_Pos  Predicted_Neg
          Actual_Pos            94              0
          Actual_Neg             0            406

.. only:: latex

    .. code::

        >>> my_model = ta.RandomForestClassifierModel()
        >>> my_model.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> my_model.test(train_frame,'Class')
          Precision: 1.0
          Recall: 1.0
          Accuracy: 1.0
          FMeasure: 1.0
          Confusion Matrix:
                            Predicted_Pos  Predicted_Neg
          Actual_Pos                94              0
          Actual_Neg                 0            406


