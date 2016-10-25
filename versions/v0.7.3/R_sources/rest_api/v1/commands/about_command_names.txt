--------------------------------------------
:doc:`Commands <index>`  About Command Names
--------------------------------------------

Command names are structured hierarchically according to entities and any intermediate scoping names.  The full name of a command is delimited with the ``/`` character.

.. image:: command_full_name.png


All commands are associated with an entity type, like a frame or a graph.  The
command name begins with the **entity type**.  The ``:`` character indicates
subtyping, where no ``:`` means all related entities inherit the command.  Example entity types::

  frame             # corresponds to all *Frame entity types
  frame:            # indicates the standard Frame entity type
  frame:vertex      # indicates the VertexFrame entity type
  model:kmeans      # indicates the KmeansModel entity type


This means the command ``frame/bin_columns`` is available on any type of
Frame object, where ``frame:vertex/add_vertices`` is only available on
``VertexFrame`` entities.  The first argument to any command is the id of an
entity instance.  This entity instance must correspond to the supported entity
type(s) indicated in the command's full name.

After the entity type, but before the name, there may be one or more intermediate names that provide additional scope.

Finally comes the name which identifies the operation.
