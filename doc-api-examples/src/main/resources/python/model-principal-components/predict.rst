Examples
--------

.. only:: html

    .. code::

        >>> my_model = atk.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model(train_frame,["1","2","3","4","5"],4)
        >>> output = my_model.predict(train_frame,c=4,t_square_index=True,name='predictedFrame')
        >>> output
             {'output_frame': Frame  <unnamed>
             row_count = 10
             schema = [1:float64, 2:float64, 3:float64, 4:float64, 5:float64, 6:float64, 7:float64, 8:float64, 9:float64, 10:float64, 11:float64, p_1:float64, p_2:float64, p_3:float64, p_4:float64, p_5:float64]
             status = Active, 't_squared_index': 20.461271109991436}
        >>> output['output_frame'].inspect(5)
            1:float64   2:float64   3:float64   4:float64   5:float64   6:float64   7:float64   8:float64   9:float64   10:float64   11:float64      p_1:float64       p_2:float64        p_3:float64       p_4:float64
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    2.6         1.7         0.3         1.5         0.8         0.7         0.7         1.5         0.7          0.6          0.6   -3.48611861057   -0.676906721481   -0.0712209575467    0.038258465339
                    3.3         1.8         0.4         0.7         0.9         0.8         0.3         1.7         0.5          0.4          0.3    -3.9181147846    0.172364229788     0.325302678392    0.320434609361
                    3.5         1.7         0.3         1.7         0.6         0.4         0.6         1.3         0.4          0.5          0.1   -4.26345228202   -0.320202930171    -0.403071100392   -0.179104737619
                    3.7         1.0         0.5         1.2         0.6         0.3         0.4         0.1         1.3          0.5          0.2   -4.00510748699    0.394308871523    -0.711886783401    0.189601218938
                    1.5         1.2         0.5         1.4         0.6         0.4         0.3         0.1         1.5          0.3          0.4   -2.30352533525   -0.935842254936    -0.221738852329    0.113376161084



.. only:: latex

    .. code::

        >>> my_model = atk.PrincipalComponentsModel(name='principalcomponentsmodel')
        >>> my_model(train_frame,["1","2","3","4","5"],4)
        >>> output = my_model.predict(train_frame,c=4,t_square_index=True,name='predictedFrame')
        >>> output
        >>> {'output_frame': Frame  <unnamed>
             row_count = 10
             schema = [1:float64, 2:float64, 3:float64, 4:float64, 5:float64, p_1:float64, p_2:float64, p_3:float64, p_4:float64]
             status = Active, 't_squared_index': 20.461271109991436}
        >>> output['output_frame'].inspect(5)
            1:float64   2:float64   3:float64   4:float64   5:float64   6:float64   7:float64   8:float64   9:float64   10:float64   11:float64      p_1:float64       p_2:float64        p_3:float64       p_4:float64
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                   2.6         1.7         0.3         1.5         0.8         0.7         0.7         1.5         0.7          0.6          0.6   -3.48611861057   -0.676906721481   -0.0712209575467    0.038258465339
                   3.3         1.8         0.4         0.7         0.9         0.8         0.3         1.7         0.5          0.4          0.3    -3.9181147846    0.172364229788     0.325302678392    0.320434609361
                   3.5         1.7         0.3         1.7         0.6         0.4         0.6         1.3         0.4          0.5          0.1   -4.26345228202   -0.320202930171    -0.403071100392   -0.179104737619
                   3.7         1.0         0.5         1.2         0.6         0.3         0.4         0.1         1.3          0.5          0.2   -4.00510748699    0.394308871523    -0.711886783401    0.189601218938
                   1.5         1.2         0.5         1.4         0.6         0.4         0.3         0.1         1.5          0.3          0.4   -2.30352533525   -0.935842254936    -0.221738852329    0.113376161084

