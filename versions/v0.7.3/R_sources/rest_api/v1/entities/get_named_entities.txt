-------------------------------------------
:doc:`Entities <index>`  Get Named Entities
-------------------------------------------

Gets list of short entries for all named entities in the entity collection.

GET /v1/:entities
=================

Request
-------

**Route** ::

  GET /v1/frames
  GET /v1/graphs
  GET /v1/models

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

Returns a list of entity entries for the given collection, where an entry is defined as...

+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| id                            | entity id (engine-assigned)                  |
+-------------------------------+----------------------------------------------+
| name                          | entity name (user-assigned)                  |
+-------------------------------+----------------------------------------------+
| url                           | url to the entity                            |
+-------------------------------+----------------------------------------------+
| entity_type                   | e.g. "frame:", "frame:vertex", "graph:",     |
|                               |  "model:kmeans"                              |
+-------------------------------+----------------------------------------------+

|

::

  Example for GET /v1/frames:
  [
    {
        "id": 7,
        "name": "super_frame",
        "url": "http://localhost:9099/v1/frames/7",
        "entity_type": "frame:"
    },
    {
        "id": 8,
        "name": "weather_frame1",
        "url": "http://localhost:9099/v1/frames/8",
        "entity_type": "frame:"
    }
  ]


**Headers**::

  Content-Length: 279
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
