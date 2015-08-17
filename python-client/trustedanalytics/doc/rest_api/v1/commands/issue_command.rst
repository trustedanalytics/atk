--------------------------------------
:doc:`Commands <index>`  Issue Command
--------------------------------------

Issue a command for execution.

POST /v1/commands
=================

Request
-------

**Route** ::

  POST /v1/commands

**Body**

+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| name                          | full name of the command                     |
+-------------------------------+----------------------------------------------+
| arguments                     | JSON object specifying the command arguments |
+-------------------------------+----------------------------------------------+

Here’s an example showing how to issue the "assign_sample" command on frame 16::

   {
     "name": "frame/assign_sample",
     "arguments": {
       "sample_labels": [
         "train",
         "test",
         "validate"
       ],
       "frame": 16,
       "random_seed": null,
       "sample_percentages": [
         0.5,
         0.3,
         0.2
       ],
       "output_column": null
     }
  }


**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json

Response
--------

**Status** ::

  200 OK

**Body**

Returns information about the command. See the Response Body for :doc:`Get Command <get_command>`. It is the same.
Note that POSTing to 'commands' creates the command and issues it for execution and immediately returns.
To determine the command progress and status, use :doc:`Get Command <get_command>`.

