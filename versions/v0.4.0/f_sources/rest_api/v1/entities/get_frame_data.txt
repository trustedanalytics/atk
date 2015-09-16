---------------------------------------
:doc:`Entities <index>`  Get Frame Data
---------------------------------------

Gets data from a frame by rows.

GET /v1/frames/:id/data?offset=:offset&count=:count
===================================================

Request
-------

**Route** ::

  GET /v1/frames/7/data?offset=0&count=10

**Parameters**

offset: index of the starting row

count: number of rows to retrieve

|

**Body**

(None)


**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json
  Accept: application/json,text/plain

|

Response
--------

**Status** ::

  200 OK

**Body**

Returns rows of data


+-------------------+-------------------------------------------------------------+
| Name              | Description                                                 |
+===================+=============================================================+
| name              | name of the operation "getRows"                             |
+-------------------+-------------------------------------------------------------+
| complete          | boolean indicating completion of data fetch                 |
+-------------------+-------------------------------------------------------------+
| result            | data: list of rows of data                                  |
|                   +-------------------------------------------------------------+
|                   | schema: row structure (column names and data types)         |
+-------------------+-------------------------------------------------------------+

|

::

    {
      "name": "getRows",
      "complete": true,
      "result": {
        "data": [[1, 2, "validate", 0], [2, 4, "validate", 0], [3, 6, "validate", 1], [2, 4, "validate", 0], [3, 6, "validate", 1], [1, 2, "validate", 0]],
        "schema": {
          "columns": [{
            "name": "c",
            "data_type": "int32",
            "index": 0
          }, {
            "name": "twice",
            "data_type": "int32",
            "index": 1
          }, {
            "name": "sample_bin",
            "data_type": "string",
            "index": 2
          }, {
            "name": "c_binned",
            "data_type": "int32",
            "index": 3
          }]
        }
      },
   }


**Headers** ::

  Content-Length: 753
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT

**Notes**

The data is considered 'unstructured', therefore taking a certain
number of rows, the rows obtained may be different every time the
command is executed, even if the parameters do not change.

[Need a note in here about performance and recommended usage]
