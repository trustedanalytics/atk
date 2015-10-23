Examples
--------
.. only:: html

    .. code::

    script
    <skip>
    >>> dataset = "/datasets/movie_data_with_names.csv"
    >>> 
    >>> schema = [("user_id", str),("movie_id", str), ("rating", atk.int32),
    >>>          ("timestamp", atk.int32), ("movie_name",str), ("release_date", str), ("splits", str)]
    >>> csv = atk.CsvFile(dataset, schema, skip_header_lines = 0)
    >>> frame = atk.Frame(csv, movie_frame_name)
    >>> cgd_cf_model = atk.CollaborativeFilteringModel(cgd_name)
    >>> cgd_cf_model_train = cgd_cf_model.train(frame, "user_id", "movie_name", "rating", "cgd",
    >>>                                         max_value = 19, regularization=0.65, min_value=1, bias_on=False)
    >>> cgd_cf_model_recommend = cgd_cf_model.recommend (uid, 10)
    </skip>

 The expected output for ALS is like this:

         ======Graph Statistics======
         Number of vertices: 10070 (left: 9569, right: 501)
         Number of edges: 302008 (train: 145182, validate: 96640, test: 60186)

         ======ALS Configuration======
         maxSupersteps: 20
         featureDimension: 3
         lambda: 0.065000
         biasOn: False
         convergenceThreshold: 0.000000
         maxVal: 5.000000
         minVal: 1.000000
         learningCurveOutputInterval: 1

         ======Learning Progress======
         superstep = 2
             cost(train) = 838.72024
             rmse(validate) = 1.220795
             rmse(test) = 1.226830
         superstep = 4
             cost(train) = 608.088979
             rmse(validate) = 1.174247
             rmse(test) = 1.180558
         superstep = 6
             cost(train) = 540.071050
             rmse(validate) = 1.166471
             rmse(test) = 1.172131
         superstep = 8
             cost(train) = 499.134869
             rmse(validate) = 1.164236
             rmse(test) = 1.169805
         superstep = 10
             cost(train) = 471.318913
             rmse(validate) = 1.163796
             rmse(test) = 1.169215
         superstep = 12
             cost(train) = 450.420300
             rmse(validate) = 1.163993
             rmse(test) = 1.169224
         superstep = 14
             cost(train) = 433.511180
             rmse(validate) = 1.164485
             rmse(test) = 1.169393
         superstep = 16
             cost(train) = 419.403410
             rmse(validate) = 1.165008
             rmse(test) = 1.169507
         superstep = 18
             cost(train) = 407.212140
             rmse(validate) = 1.165425
             rmse(test) = 1.169503
         superstep = 20
             cost(train) = 396.281966
             rmse(validate) = 1.165723
             rmse(test) = 1.169451

