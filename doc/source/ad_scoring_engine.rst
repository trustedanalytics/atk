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


