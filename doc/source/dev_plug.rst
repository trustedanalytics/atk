.. _dev_plug:

.. index::
    single: plugin
    single: extend
    single: develop
    single: enhance

======================
Plugin Authoring Guide
======================

.. contents:: Table of Contents
    :local:
    :backlinks: none

------------
Introduction
------------

|PACKAGE| provides an extensibility framework that allows new commands and algorithms to be added as plug-ins to the application.

Plug-ins do not require the author to have a deep understanding the REST server, execution engine, or any of its libraries. The framework will create the code necessary to call the plugin through the REST API as well as generate Python client code, leaving the author to focus on the execution logic.

Plug-ins are written in Scala and have a few requirements that follow this pattern:

1. Declare an arguments case class.
2. Declare an return value case class.
3. Define JSON serialization (most of which is provided through library calls).
4. Write the execution logic.


----------------------
When to Write a Plugin
----------------------

Many of the operations and algorithms that only address a single can be written
in Python using Python UDFs in the client.

Anytime there is a new function such as an analytical or
:term:`machine learning` algorithm that would be desirable to publish for use
via the Python client or REST server, use a CommandPlugin.

-----------------------
Plugin Support Services
-----------------------

Plugin Life Cycle
=================

Each invocation resulting from a user action or other source will provide an
execution context object that encapsulates the arguments passed by the user as well as other relevant metadata.

Logging and Error Handling
==========================

Errors that occur while running a plug-in will be reported in the
same way that internal errors within |PACKAGE| are normally reported.

Defaulting Arguments
====================

Authors should represent arguments that are not required using default values in their case classes.

Configuration for commands and queries should be included in the Typesafe
Config configuration file associated with the application (defaults can be
provided by a reference.conf in the plugin's deployment jar).
Configuration details are discussed in the "Configuration" section below.
Plugins have access to the configuration, but only the section of it that
contains settings that are relevant.
For example, the Loopy Belief Propagation plugin gets its configuration from
'trustedanalytics.atk.giraph-plugins.command.graph.ml.loopy_belief_propagation.config'.

Execution Flow
==============

.. image:: dev_plug_1.*
    :width: 80 %
    :align: center

------------------------
Creating a CommandPlugin
------------------------

Naming
======

Naming the command correctly is crucial for the usability of the system.
The Python client creates Python functions to match the commands in the engine,
and it places them and names them in accordance with the name specified for the
plugin.

Name components are separated by slashes.
For instance, the command that drops columns from a frame is called
frame/drop_column.
The Python client sees that name, knows that frame commands are associated
with the Frame class, and therefore generates a function
named drop_column on the Frame.
When the user calls that function, its arguments will be converted to JSON,
sent to the REST server, and then on to the engine for processing.
The results from the engine flow back through the REST server, and are
converted back to Python objects.

If the name of the command contains more than one slash, the Python client will
create intermediate objects that allow functions to be grouped logically
together.
For example, if the command is named frame/ml/my_new_algorithm then the method created in the Python
client could be accessed on a frame *f* using ``f.ml.my_new_algorithm()``.
Commands can be nested as deeply as needed, any number of intermediary objects
will be created automatically so the object model of the frame or graph matches
the command tree structure defined by the command names in the system.

REST Input and Output
=====================

Each command plug-in should define two case classes: one for
arguments, and one for return value.
The plug-in framework will ensure that the user's Python (or JSON) commands are
converted into an instance of the argument class, and the output from the
plug-in will also be converted back to Python (or JSON) for storage in the
command execution record for later return to the client. author must provide some serialization hints.

Frame and Graph References
==========================

The commands associated with a frame or graph accept the frame
or graph on which they should operate as the parameter.
Use the class org.trustedanalytics.atk.domain.frame.FrameReference to represent
frames, and org.trustedanalytics.atk.domain.graph.GraphReference to represent
graphs.

Use a FrameReference as the type, and place this parameter first in the case
class definition if it is desired that this parameter is filled by the Frame
instance whose method is being invoked by the user.
Similarly, if the method is on a graph, using  a GraphReference in the first
position will do the trick for graph instances.

Single Value Results
====================

The result returned by command plugins can be as complex as needed.
It can also be very simple — for example, a single floating point value.
Since the result type of the plugin must be a case class, the convention is to
return a case class with one field, which must be named "value".
When the client receives such a result, it should extract and return the single
value.

---------------------------
Creating a Jar with Plugins
---------------------------

Plugins are deployed in jar files that contain the plugin class,
its argument and result classes, and any supporting classes it needs.

On application start up, the application will query all the plugin jar files
it knows about to see what plugins they provide.

See the module-loader documentation and the example-plugins module for to learn
more about creating plugin modules.

----------
Deployment
----------

Plug-Ins should be installed in the system using jar files.
Jars that are found in the server's lib directory will be available to be
loaded based on configuration.
The plug-ins that will be installed must be listed in the atk-plugin.conf
file.

In the future, plugin discovery may be further automated, and it may also be
possible to add a plugin without restarting the server.


