----------------------------
:doc:`REST API <index>` Info
----------------------------

Basic server information supplying API versions.

GET /info
=========

Request
-------

**Route** ::

  GET /info

**Body**

(None)

**Headers**

(None)

|

Response
--------

**Status** ::

  200 OK

**Body**

Returns server information and API versions.

::

   {
    "name": "Trusted Analytics",
    "identifier": "ia",
    "versions": [
        "v1"
    ]
  }


**Headers** ::

  Content-Length: 75
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
