Examples
--------
Predict using a Principal Components Model

.. only:: html

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model.train(train_frame,["1","2","3"],k=3)
        >>> predict_output = my_model.predict(train_frame, mean_centered=True, t_square_index=True, name='predictedFrame')

        >>> output.inspect(3)
            1:float64   2:float64   3:float64   4:float64   5:float64   6:float64   7:float64   8:float64   9:float64   10:float64   11:float64      p_1:float64       p_2:float64        p_3:float64       t_squared_index:float64
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    2.6         1.7         0.3         1.5         0.8         0.7         0.7         1.5         0.7          0.6          0.6   0.376046860448     -0.188093098136     0.0104759334223    0.0602987596369
                    3.3         1.8         0.4         0.7         0.9         0.8         0.3         1.7         0.5          0.4          0.3   -0.32590292549     -0.164760657542     0.139789874036     0.204603929413
                    3.5         1.7         0.3         1.7         0.6         0.4         0.6         1.3         0.4          0.5          0.1   -0.513207506722    -0.0508831966974    0.0304787686474    0.0782853147235


.. only:: latex

    .. code::

        >>> my_model = ta.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model.train(train_frame,["1","2","3"],k=3)
        >>> predict_output = my_model.predict(train_frame, mean_centered=True, t_square_index=True, name='predictedFrame')

        >>> output.inspect(3)
            1:float64   2:float64   3:float64   4:float64   5:float64   6:float64   7:float64   8:float64   9:float64   10:float64   11:float64      p_1:float64       p_2:float64        p_3:float64       t_squared_index:float64
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    2.6         1.7         0.3         1.5         0.8         0.7         0.7         1.5         0.7          0.6          0.6   0.376046860448     -0.188093098136     0.0104759334223    0.0602987596369
                    3.3         1.8         0.4         0.7         0.9         0.8         0.3         1.7         0.5          0.4          0.3   -0.32590292549     -0.164760657542     0.139789874036     0.204603929413
                    3.5         1.7         0.3         1.7         0.6         0.4         0.6         1.3         0.4          0.5          0.1   -0.513207506722    -0.0508831966974    0.0304787686474    0.0782853147235

