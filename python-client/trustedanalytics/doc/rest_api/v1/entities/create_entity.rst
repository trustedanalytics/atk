--------------------------------------
:doc:`Entities <index>`  Create Entity
--------------------------------------

Creates a new entity, like a frame, graph, or model.

POST /v1/:entities/
===================

Request
-------

**Route** ::

  POST /v1/frames/
  POST /v1/graphs/
  POST /v1/models/

**Body**

+-------------------------------+----------------------------------------------+-----------+-----------------------------+------------------+
| Name                          | Description                                  | Default   | Valid Values                |  Example Values  |
+===============================+==============================================+===========+=============================+==================+
| name                          | name for the entity                          | null      | alphanumeric UTF-8 strings  | 'weather_frame1' |
+-------------------------------+----------------------------------------------+-----------+-----------------------------+------------------+

::

  {
    "name": "weather_frame1"
 }

**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json

Response
--------

**Status** ::

  200 OK

**Body**

Returns a summary of the entity.


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
    "name": "weather_frame1",
    "uri": "frames/8",
    "schema": {
        "columns": []
    },
    "row_count": 0,
    "links": [
        {
            "rel": "self",
            "uri": "http://localhost:9099/v1/frames/8",
            "method": "GET"
        }
    ],
    "entity_type": "frame:",
    "status": "Active"
  }


**Headers** ::

  Content-Length: 279
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
  Server: spray-can/1.3.1
  build_id: TheReneNumber


