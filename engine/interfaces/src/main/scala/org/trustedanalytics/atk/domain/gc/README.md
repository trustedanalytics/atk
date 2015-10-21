Garbage Collection
==================

ATK has a garbage collection service which runs in a background thread
periodically.  There are also plugins which expose manual execution through
the public API.


Status of Entities
------------------

An entity (i.e. Frame, Graph, or Model) exists in one of these states:

1.  **Active** - available to the user
2.  **Dropped** - no longer available to the user
3.  **Finalized** - not available and its data has been erased from disk or store

Phases of Garbage Collection
----------------------------

The Garbage Collector does 2 things (or has 2 phases)

1.  Looks for all "stale" entities and drops them (status becomes DROPPED)
2.  Finalizes all DROPPED entities (status becomes FINALIZED and their
    storage space is reclaimed)

The 2 phases could potentially run on different schedules, however, for now
there is only one schedule thread and both phases run during any given
scheduled execution.

"Stale"
-------
GC uses polarity "stale" rather than "live".  Requirements of a "stale" entity:
1.  Has status ACTIVE, but...
2.  Has no name
3.  Is not owned by another entity
4.  Has not been "accessed" for a certain period of time

"Accessed"
----------
GC considers "accessed" to mean any one of the following:
1. Command executed with the entity as the subject (self)
2. Command executed with the entity as an argument
3. Entity data is queried (i.e. 'take')

Exceptions:
4. Entity metadata query does not count as access.
    a. Note: Entity metadata cannot be directly modified, except for name,
       which plays into "stale" on its own
5. Appearance in *get_{entity}_names* does not count as access

In the code, the term "lastReadDate" is the marked for "accessed" and is
triggered around loads and stores from/to the entity's backing store.


More Entity Drop Rules
----------------------

1.  Dropping entities (whether initiated by USER or GC) also drops entities
    owned by that entity.  Examples inclucde a frame's error frame or the frames
    which compose a seamless graph.  Also note:
    a. The owned entity is typically not named, but even if it is, it will
       still get dropped.
    b. ATK does not support multiple owners

2.  If an entity is owned, it can only be dropped via its owner:
    a. If the owned entity itself is dropped, described in #1
    b. The owner entity initiates the drop.  Examples:
        i.  a seamless graph can drop a vertex frame via its API
            (but that vertex frame cannot be dropped directly)
       ii.  introduce an explicit drop_error_frame() function on frames


**A note about terminology:**

There are many operations which _delete_ "things".  We reserve the word
_delete_ to be used strictly in the CRUD sense:
* if we _delete_ an entity from the metastore, the entity record is removed;
* if we _delete_ a file, it’s gone.

Whereas, _drop_ means move entity status to "Dropped", and_finalize_ means
we have deleted an entity’s data and moved its status to "Finalized".  We
also favour the word _erase_ to mean deleting the data.

We could potentially _undrop_ but we could never _unfinalize_.
  (* Note there is no 'undrop' feature implemented, nor originally planned)

There is no "*delete*" or "*finalize*" in the public API.

So, in summary, in the code:

* If we _drop_ a frame, its entity status goes to Dropped
* If we _finalize_ a frame, its data is erased and its entity status goes
  to "Finalized"
* If we _delete_ a frame, its data is erased (if present) and its entity
  is removed from the metastore


Conf
----
Frequency of gc execution is set by engine-core's reference conf:
  ex. gc.interval = "30 minutes"

Default age determining staleness is also set in the reference conf:
  ex. gc.stale-age = "7 days"


Manual Execution
----------------
There are two commands available in ATK, exposed using plugins.  Each runs
a phase of gc manually:  1. drop_stale and 2. finalize_dropped

```
import trustedanalytics.core.admin as admin
admin.drop_stale("2 hours")  # drops everything which hasn't been accessed in
                             # the last 2 hours, and that doesn't have a name

admin.finalize_dropped()     # immediately erases all data held by dropped
                             # entities
```

