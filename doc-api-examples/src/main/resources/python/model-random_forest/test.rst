Examples
--------
Test the performance of a trained random forest classifier

.. only:: html

    .. code::

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> rf_classifier.test(train_frame,'Class')
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

        >>> rf_classifier = ta.RandomForestClassifierModel()
        >>> rf_classifier.train(train_frame,'Class',['Dim_1','Dim_2'],num_classes=2)
        >>> rf_classifier.test(train_frame,'Class')
          Precision: 1.0
          Recall: 1.0
          Accuracy: 1.0
          FMeasure: 1.0
          Confusion Matrix:
                            Predicted_Pos  Predicted_Neg
          Actual_Pos                94              0
          Actual_Neg                 0            406


