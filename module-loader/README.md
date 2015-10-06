
Module Loader
=============

The Atk Module Loader provides the ClassLoader isolation needed by Atk and Atk plugins.


What is a Module
================

A module is basically defined by a name, a parent module, and a list of jar dependencies.

A module has a ClassLoader that includes the list of jar dependencies and whose parent is the parent modules ClassLoader.

Both Modules and jars are searched for in the module-search-path.


Configuring a Module
====================

Modules are configured by adding an atk-module.conf file to the root of a jar

Modules typically have two styles:

1. Modules that have a parent.  The parent relationship between modules means the ClassLoaders for the modules will 
share a parent relationship.  "system" is a special parent value reserved for the module loader itself.

```
atk.module {
  name = "my-name"
  parent = "my-parent" 
}
```

2. Modules that are a member-of other modules.  The member-of relationship means the modules will be combined at 
runtime into a single module definition with a single classloader.  This is used when Spark plugins are spread
accross multiple modules that will need to share the same runtime environment.

```
atk.module {
  name = "my-name"
  member-of = "another-module" 
}
```

Modules cannot define both a parent and a member-of value.  Usually, a module's name will be the same as its jar name.


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



