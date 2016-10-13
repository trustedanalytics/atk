-----------------------------------
:doc:`Entities <index>`  Get Entity
-----------------------------------

Gets information about specific entity, like a frame, graph, or model.  There are two options: get by id or get by name.

GET /v1/:entities/:id
=====================

GET /v1/:entities?name=
=======================

Request
-------

**Route** ::

  GET /v1/frames/3
  GET /v1/graphs/1
  GET /v1/models/4
  GET /v1/frames?name=weather_frame1
  GET /v1/graphs?name=networkB

**Body**

(None)

**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json

Response
--------

**Status** ::

  200 OK

**Body**

Returns information about the entity


+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| uri                           | entity id (engine-assigned)                  |
+-------------------------------+----------------------------------------------+
| name                          | entity name (user-assigned)                  |
+-------------------------------+----------------------------------------------+
| links                         | links to the entity                          |
+-------------------------------+----------------------------------------------+
| entity_type                   | e.g. "frame:", "frame:vertex", "graph:"      |
+-------------------------------+----------------------------------------------+
| status                        | status: Active, Deleted, Deleted_Final       |
+-------------------------------+----------------------------------------------+

|

Extra fields specific to frames:

+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| schema                        | frame schema info                            |
|                               |                                              |
|                               |  columns: [ (name, type) ]                   |
+-------------------------------+----------------------------------------------+
| row_count                     | number of rows in the frame                  |
+-------------------------------+----------------------------------------------+

|

::

   {
    "uri": "frames/7",
    "name": "super_frame",
    "entity_type": "frame:",
    "status": "Active"
    "links": [{
      "rel": "self",
      "uri": "http://localhost:9099/v1/frames/7",
      "method": "GET"
    }],
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
    },
    "row_count": 8675309,
  }


**Headers** ::

  Content-Length: 279
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
  Server: spray-can/1.3.1
  build_id: TheReneNumber

