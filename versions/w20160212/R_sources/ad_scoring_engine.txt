.. _ad_scoring_engine:
Scoring Engine
==============

This section covers deployment and running the scoring engine.


create a Scoring Engine Instance
--------------------------

In the TAP web site:

1) Navigate to **Services -> Marketplace**.
2) Select **scoring_engine** => **Create new isntance**.
3) Fill in an instance name of your choice *(given below as **my-svm-model**)*.
4) Select **+ Add variable**.
5) Fill in two values: key **TAR_ARCHIVE**; value is the URI of the model you wish to use.

You will be able to see your scoring engine under the Applications page.


Scoring Client
--------------

Below is a sample python script to connect to the scoring engine:

.. code::

    >>> import requests
    >>> import json
    >>> headers = {'Content-type': 'application/json',
    ...            'Accept': 'application/json,text/plain'}
    >>> r = requests.post('http://my-svm-model:9099/v1/score?data=2,17,-6', headers=headers)
    >>> r.text
    list(1)


Posting Requests to Scoring Engine
----------------------------------

Below are a couple of examples of posting a request to a scoring engine containing a LibSvm Model, and its response

version 1 of Scoring Engine supporting strings for requests and response:

request from a python client with String Input scoring a record:
r = requests.post('http://localhost:9100/v1/score?data=-1,-1, -1', headers=headers)
String response:
'-1.0'

version 2 of Scoring Engine supporting Json for requests and responses:

request from a python client with Json Input scoring a record:
r = requests.post("http://localhost:9100/v2/score", json={"records": [{"b": 1, "c": 2, "d": 3}]})
Json response:
u'{"Model Details":{"model_type":"LibSvm Model","model_class":"org.trustedanalytics.atk.scoring.models.LibSvmModel","model_reader":"org.trustedanalytics.atk.scoring.models.LibSvmModelReaderPlugin","custom_values":{}},"Input":[{"name":"b","value":"Double"},{"name":"c","value":"Double"},{"name":"d","value":"Double"}],"output":[[1.0,2.0,3.0,-1.0]]}'


