Examples
--------
.. only:: html

    .. code::

    script

    ta.connect()
    dataset = "/datasets/movie_data_with_names.csv"

    schema = [("user_id", str),("movie_id", str), ("rating", atk.int32),
             ("timestamp", atk.int32), ("movie_name",str), ("release_date", str), ("splits", str)]
    csv = atk.CsvFile(dataset, schema, skip_header_lines = 0)
    frame = atk.Frame(csv, movie_frame_name)
    cgd_cf_model = atk.CollaborativeFilteringModel(cgd_name)
    cgd_cf_model_train = cgd_cf_model.train(frame, "user_id", "movie_name", "rating", "cgd",
                                            max_value = 19, regularization=0.65, min_value=1, bias_on=False)
    cgd_cf_model_recommend = cgd_cf_model.recommend (uid, 10)

.. only:: latex

    .. code::

    ta.connect()
    dataset = "/datasets/movie_data_with_names.csv"

    schema = [("user_id", str),("movie_id", str), ("rating", atk.int32),
             ("timestamp", atk.int32), ("movie_name",str), ("release_date", str), ("splits", str)]
    csv = atk.CsvFile(dataset, schema, skip_header_lines = 0)
    frame = atk.Frame(csv, movie_frame_name)
    cgd_cf_model = atk.CollaborativeFilteringModel(cgd_name)
    cgd_cf_model_train = cgd_cf_model.train(frame, "user_id", "movie_name", "rating", "cgd",
                                            max_value = 19, regularization=0.65, min_value=1, bias_on=False)
    cgd_cf_model_recommend = cgd_cf_model.recommend (uid, 10)


The expected output for ALS is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 10070 (left: 9569, right: 501)\nNumber of edges: 302008 (train: 145182, validate: 96640, test: 60186)\n\n======ALS Configuration======\nmaxSupersteps: 20\nfeatureDimension: 3\nlambda: 0.065000\nbiasOn: False\nconvergenceThreshold: 0.000000\nmaxVal: 5.000000\nminVal: 1.000000\nlearningCurveOutputInterval: 1\n\n======Learning Progress======\nsuperstep = 2\tcost(train) = 838.720244\trmse(validate) = 1.220795\trmse(test) = 1.226830\nsuperstep = 4\tcost(train) = 608.088979\trmse(validate) = 1.174247\trmse(test) = 1.180558\nsuperstep = 6\tcost(train) = 540.071050\trmse(validate) = 1.166471\trmse(test) = 1.172131\nsuperstep = 8\tcost(train) = 499.134869\trmse(validate) = 1.164236\trmse(test) = 1.169805\nsuperstep = 10\tcost(train) = 471.318913\trmse(validate) = 1.163796\trmse(test) = 1.169215\nsuperstep = 12\tcost(train) = 450.420300\trmse(validate) = 1.163993\trmse(test) = 1.169224\nsuperstep = 14\tcost(train) = 433.511180\trmse(validate) = 1.164485\trmse(test) = 1.169393\nsuperstep = 16\tcost(train) = 419.403410\trmse(validate) = 1.165008\trmse(test) = 1.169507\nsuperstep = 18\tcost(train) = 407.212140\trmse(validate) = 1.165425\trmse(test) = 1.169503\nsuperstep = 20\tcost(train) = 396.281966\trmse(validate) = 1.165723\trmse(test) = 1.169451'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 10070 (left: 9569, right: 501)\n
        Number of edges: 302008 (train: 145182, validate: 96640, test: 60186)\n
        \n
        ======ALS Configuration======\n
        maxSupersteps: 20\n
        featureDimension: 3\n
        lambda: 0.065000\n
        biasOn: False\n
        convergenceThreshold: 0.000000\n
        maxVal: 5.000000\n
        minVal: 1.000000\n
        learningCurveOutputInterval: 1\n
        \n
        ======Learning Progress======\n
        superstep = 2\t
            cost(train) = 838.720244\t
            rmse(validate) = 1.220795\t
            rmse(test) = 1.226830\n
        superstep = 4\t
            cost(train) = 608.088979\t
            rmse(validate) = 1.174247\t
            rmse(test) = 1.180558\n
        superstep = 6\t
            cost(train) = 540.071050\t
            rmse(validate) = 1.166471\t
            rmse(test) = 1.172131\n
        superstep = 8\t
            cost(train) = 499.134869\t
            rmse(validate) = 1.164236\t
            rmse(test) = 1.169805\n
        superstep = 10\t
            cost(train) = 471.318913\t
            rmse(validate) = 1.163796\t
            rmse(test) = 1.169215\n
        superstep = 12\t
            cost(train) = 450.420300\t
            rmse(validate) = 1.163993\t
            rmse(test) = 1.169224\n
        superstep = 14\t
            cost(train) = 433.511180\t
            rmse(validate) = 1.164485\t
            rmse(test) = 1.169393\n
        superstep = 16\t
            cost(train) = 419.403410\t
            rmse(validate) = 1.165008\t
            rmse(test) = 1.169507\n
        superstep = 18\t
            cost(train) = 407.212140\t
            rmse(validate) = 1.165425\t
            rmse(test) = 1.169503\n
        superstep = 20\t
            cost(train) = 396.281966\t
            rmse(validate) = 1.165723\t
            rmse(test) = 1.169451'}

Report may show zero edges and/or vertices if parameters were supplied wrong,
or if the graph was not the expected input:

.. code::

    ======Graph Statistics======
    Number of vertices: 12673 (left: 12673, right: 0)
    Number of edges: 0 (train: 0, validate: 0, test: 0)

The expected output for CGD is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 20140 (left: 10070, right: 10070)\nNumber of edges: 604016 (train: 554592, validate: 49416, test: 8)\n\n======CGD Configuration======\nmaxSupersteps: 20\nfeatureDimension: 3\nlambda: 0.065000\nbiasOn: false\nconvergenceThreshold: 0.000000\nnumCGDIters: 3\nmaxVal: Infinity\nminVal: -Infinity\nlearningCurveOutputInterval: 1\n\n======Learning Progress======\nsuperstep = 2\tcost(train) = 21828.395401\trmse(validate) = 1.317799\trmse(test) = 3.663107\nsuperstep = 4\tcost(train) = 18126.623261\trmse(validate) = 1.247019\trmse(test) = 3.565567\nsuperstep = 6\tcost(train) = 15902.042769\trmse(validate) = 1.209014\trmse(test) = 3.677774\nsuperstep = 8\tcost(train) = 14274.718100\trmse(validate) = 1.196888\trmse(test) = 3.656467\nsuperstep = 10\tcost(train) = 13226.419606\trmse(validate) = 1.189605\trmse(test) = 3.699198\nsuperstep = 12\tcost(train) = 12438.789925\trmse(validate) = 1.187416\trmse(test) = 3.653920\nsuperstep = 14\tcost(train) = 11791.454643\trmse(validate) = 1.188480\trmse(test) = 3.670579\nsuperstep = 16\tcost(train) = 11256.035422\trmse(validate) = 1.187924\trmse(test) = 3.742146\nsuperstep = 18\tcost(train) = 10758.691712\trmse(validate) = 1.189491\trmse(test) = 3.658956\nsuperstep = 20\tcost(train) = 10331.742207\trmse(validate) = 1.191606\trmse(test) = 3.757683'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 20140 (left: 10070, right: 10070)\n
        Number of edges: 604016 (train: 554592, validate: 49416, test: 8)\n
        \n
        ======CGD Configuration======\n
        maxSupersteps: 20\n
        featureDimension: 3\n
        lambda: 0.065000\n
        biasOn: false\n
        convergenceThreshold: 0.000000\n
        numCGDIters: 3\n
        maxVal: Infinity\n
        minVal: -Infinity\n
        learningCurveOutputInterval: 1\n
        \n
        ======Learning Progress======\n
        superstep = 2\tcost(train) = 21828.395401\t
            rmse(validate) = 1.317799\trmse(test) = 3.663107\n
        superstep = 4\tcost(train) = 18126.623261\t
            mse(validate) = 1.247019\trmse(test) = 3.565567\n
        superstep = 6\tcost(train) = 15902.042769\t
            mse(validate) = 1.209014\trmse(test) = 3.677774\n
        superstep = 8\tcost(train) = 14274.718100\t
            mse(validate) = 1.196888\trmse(test) = 3.656467\n
        superstep = 10\tcost(train) = 13226.419606\t
            mse(validate) = 1.189605\trmse(test) = 3.699198\n
        superstep = 12\tcost(train) = 12438.789925\t
            mse(validate) = 1.187416\trmse(test) = 3.653920\n
        superstep = 14\tcost(train) = 11791.454643\t
            mse(validate) = 1.188480\trmse(test) = 3.670579\n
        superstep = 16\tcost(train) = 11256.035422\t
            mse(validate) = 1.187924\trmse(test) = 3.742146\n
        superstep = 18\tcost(train) = 10758.691712\t
            mse(validate) = 1.189491\trmse(test) = 3.658956\n
        superstep = 20\tcost(train) = 10331.742207\t
            mse(validate) = 1.191606\trmse(test) = 3.757683'}

Report may show zero edges and/or vertices if parameters were supplied
wrong, or if the graph was not the expected input:

.. code::

    ======Graph Statistics======
    Number of vertices: 12673 (left: 12673, right: 0)
    Number of edges: 0 (train: 0, validate: 0, test: 0)

