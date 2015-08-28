
Module Loader
=============

The Atk Module Loader provides the ClassLoader isolation needed by Atk and Atk plugins.


What is a Module
================

A module is basically defined by a name, a parent module, and a list of jar dependencies.

A module has a ClassLoader that includes the list of jar dependencies and whose parent is the parent modules ClassLoader.

Both Modules and jars are searched for in the module-search-path.


Known Limitations / Possible Future Work
========================================
* Assumes jar names are unique.
* Supports a single parent relationship.  If there was a use case, we could possibly support more complicated
  relationships (e.g. a module having a list of parents).
* Plugins are assumed to be trusted code.  We could restrict them (Java SecurityManagers, additional ClassLoader without
  full access to config, etc) but since plugin authors will need so much access to the system to install a plugin in
  the first place it isn't clear there is value in trying to restrict them.
* Why didn't you use OSGi?  We just needed something dirt-simple that met our needs and that we could deliver quickly.
  We might consider other frameworks later.
* Atk should probably be broken up into more modules.  For example, instead of plugins having a direct parent
  relationship with the engine their parent could be some kind of interfaces module specific to their runtime
  environment (e.g Spark).



