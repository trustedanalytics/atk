.. _ad_scoring_engine:
Scoring Engine
==============

This section covers deployment and running the scoring engine.


Create a Scoring Engine Instance
--------------------------------

In the TAP console:

1) Navigate to **Services -> Marketplace**.
2) Select **scoring_engine** => **Create new isntance**.
3) Fill in an instance name of your choice *(given below as **my-svm-model**)*.
4) Select **+ Add variable**.
5) Fill in two values: key **TAR_ARCHIVE**; value is the URI of the model you wish to use.

You will be able to see your scoring engine under the Applications page.


Scoring Client
--------------

Below is a sample Python script to send requests to the scoring engine:

.. code::

    $ python[2.7]
    >>> import requests
    >>> headers = {'Content-type': 'application/json',
    ...            'Accept': 'application/json,text/plain'}


    # Posting a request to version 1 of Scoring Engine supporting strings for requests and response:
    >>> r = requests.post('http://my-svm-model.demotrustedanalytics.com/v1/score?data=2,17,-6', headers=headers)
    >>> r.text
    u'-1.0'

    # Posting a request to version 1 with multiple records to score:
    >>> r = requests.post('http://my-svm-model.demotrustedanalytics.com/v1/score?data=2,17,-6&data=0,0,0', headers=headers)
    >>> r.text
    u'-1.0,1.0'

    # Posting a request to version 2 of Scoring Engine supporting Json for requests and responses. In the following example, 'tr_row' and 'tr_col' are the names of the observation columns that the model was trained on:
    >>> r = requests.post("http://my-svm-model.demotrustedanalytics.com/v2/score", json={"records": [{"tr_row": 1.0, "tr_col": 2.6}]})
    >>> r.text
    u'{"data":[{"tr_row":1.0,"tr_col":2.6,"Prediction":-1.0}]}'

    # posting a request to version 2 with multiple records to score:
    >>> r = requests.post("http://my-svm-model.demotrustedanalytics.com/v2/score", json={"records": [{"tr_row": 1.0, "tr_col": 2.6},{"tr_row": 3.0, "tr_col": 0.6} ]})
    >>> r.text
    u'{"data":[{"tr_row":1.0,"tr_col":2.6,"Prediction":-1.0},{"tr_row":3.0,"tr_col":0.6,"Prediction":-1.0}]}'

    # posting a request to get the metadata about the model
    >>> r =requests.get('http://my-svm-model.demotrustedanalytics.com/v2/metadata')
    >>> r.text
    u'{"model_details":{"model_type":"LibSvm Model","model_class":"org.trustedanalytics.atk.scoring.models.LibSvmModel","model_reader":"org.trustedanalytics.atk.scoring.models.LibSvmModelReaderPlugin","custom_values":{}},"input":[{"name":"tr_row","value":"Double"},{"name":"tr_col","value":"Double"}],"output":[{"name":"tr_row","value":"Double"},{"name":"tr_col","value":"Double"},{"name":"Prediction","value":"Double"}]}'

Header 
------

-   `Autoregressive Exogenous Model (ARX)  <python_api/models/model-arx/publish.html>`_
-   `Autoregressive Integrated Moving Average Model (ARIMA) <python_api/models/model-arima/publish.html>`_
-   `K Means <python_api/models/model-k_means/publish.html>`_
-   `Label Propagation <python_api/graphs/graph-/label_propagation.html>`_
-   `Latent Dirichlet Allocation (LDA) <python_api/models/model-lda/publish.html>`_
-   `Lib SVM <python_api/models/model-libsvm/publish.html>`_
-   `Linear Regression <python_api/models/model-linear_regression/publish.html>`_
-   `Loopy Belief Propagation <python_api/graphs/graph-/loopy_belief_propagation.html>`_
-   `Naive Bayes <python_api/models/model-naive_bayes/publish.html>`_
-   `Principal Component Analysis <python_api/models/model-principal_components/publish.html>`_
-   `Random Forest Classifier <python_api/models/model-random_forest_classifier/publish.html>`_
-   `Random Forest Regressor <python_api/models/model-random_forest_regressor/publish.html>`_
-   `SVM with SGD <python_api/models/model-svm/publish.html>`_

