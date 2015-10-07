------------------------------------
:doc:`Commands <index>`  Get Command
------------------------------------

Gets information about a specific command.

GET /v1/commands/:id
====================

Request
-------

**Route** ::

  GET /v1/commands/25

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

Returns information about the command


+-------------------------------+--------------------------------------------------+
| Name                          | Description                                      |
+===============================+==================================================+
| id                            | command instance id (engine-assigned)            |
+-------------------------------+--------------------------------------------------+
| name                          | command name                                     |
+-------------------------------+--------------------------------------------------+
| correlation_id                | correlation id                                   |
+-------------------------------+--------------------------------------------------+
| links                         | links to the command                             |
+-------------------------------+--------------------------------------------------+
| arguments                     | the arguments that were passed to the command    |
+-------------------------------+--------------------------------------------------+
| progress                      | command execution progress                       |
|                               | ``progress``: percentage complete                |
|                               | ``tasks_info``: info about each task in the      |
|                               | command including number of retries              |
+-------------------------------+--------------------------------------------------+
| complete                      | boolean indicating if the command has finished   |
+-------------------------------+--------------------------------------------------+
| result                        | the return data from command completion.         |
|                               | (field will not appear until 'complete' is true) |
+-------------------------------+--------------------------------------------------+

|

Example response body for command 18, 'assign_sample' on frame 16 which is in the middle of execution::

   {
     "id": 18,
     "name": "frame/assign_sample",
     "correlation_id": "",
     "arguments": {
       "sample_labels": ["train", "test", "validate"],
       "frame": 16,
       "random_seed": null,
       "sample_percentages": [0.5, 0.3, 0.2],
       "output_column": null
     },
     "progress": [{
        "progress": 33.33000183105469,
        "tasks_info": {
          "retries": 0
        }
     }],
     "complete": false,
     "links": [{
       "rel": "self",
       "uri": "http://localhost:9099/v1/commands/18",
       "method": "GET"
     }]
  }


Example response body for command 17, a 'load' on frame 16 which has completed::
  
   {
     "id": 17,
     "name": "frame:/load",
     "correlation_id": "3d074058-54bd-4170-a8a7-2219e6e3a894",
     "arguments": {
       "source": {
         "source_type": "file",
         "parser": {
           "name": "builtin/line/separator",
           "arguments": {
             "separator": ",",
             "skip_rows": 0,
             "schema": {
               "columns": [["c", "int32"], ["number", "unicode"]]
             }
           }
         },
         "data": null,
         "uri": "/join_left.csv"
       },
       "destination": 16
     },
     "progress": [{
       "progress": 100.0,
       "tasks_info": {
         "retries": 0
       }
     }],
     "complete": true,
     "result": {
       "id": 16,
       "name": "super_frame",
       "schema": {
         "columns": [{
           "name": "c",
           "data_type": "int32",
           "index": 0
         }, {
           "name": "number",
           "data_type": "string",
           "index": 1
         }]
       },
       "status": 1,
       "created_on": "2015-05-15T14:58:23.369-07:00",
       "modified_on": "2015-05-15T14:58:35.272-07:00",
       "storage_format": "file/parquet",
       "storage_location": "hdfs://paulsimon.hf.trustedanalytics.com/user/atkuser/trustedanalytics/frames/16",
       "row_count": 3,
       "modified_by": 1,
       "materialized_on": "2015-05-15T14:58:32.611-07:00",
       "materialization_complete": "2015-05-15T14:58:35.258-07:00",
       "last_read_date": "2015-05-15T14:58:23.369-07:00",
       "uri": "frames/16",
       "entity_type": "frame:"
     },
     "links": [{
       "rel": "self",
       "uri": "http://localhost:9099/v1/commands/17",
       "method": "GET"
     }]
  }

|

**Headers** ::

  Content-Length: 405
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT


**Note**

[Some notes could go here recommending polling strategies, and more info about the progress field]

