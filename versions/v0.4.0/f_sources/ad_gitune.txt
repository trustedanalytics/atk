.. index::
    single: Giraph
    single: performance
    single: tuning
    single: benchmarking

Giraph Performance Tuning and Benchmarking
==========================================

Apache Giraph is a  Hadoop-based graph-processing framework that is modeled
after Google's Pregel.
Giraph jobs are memory-hungry applications because it attempts to load the
entire graph in-memory.
Giraph jobs have no reducers.
To improve memory-related issues:

*   Allocate all available memory to the job, in other words, set the total JVM
    heap size available to mappers in $HADOOP_HOME/conf/mapred-site
    (mapreduce.tasktracker.map.tasks.maximum.* maximum heap size in
    mapred.map.child.java.opts) so it does not exceed available memory for the
    node, and leaves room for other daemons running on the nodes, for example,
    datanode, regionserver, and tasktracker.
*   Allocate at least 4GB to each map tasks, but this value depends
    heavily on the size of the graph.
*   The maximum number of workers available is the number of map slots
    available in the cluster - 1 (in other words, the number of nodes times
    the mapreduce.tasktracker.map.tasks.maximum - 1).
    One master and the rest are slaves.
*   When writing Giraph jobs, keep the compute function small and minimize
    vertex memory footprint.
    There is a patch for spilling data to disk but it does not appear to be
    working very well at the moment.

To solve performance problems, try upgrading to the latest version of
Giraph, which might include patches that address performance concerns.
For example, Facebook submitted a large number of patches that allow Giraph to
run at scale
`http link <http://www.facebook.com/notes/facebook-engineering/scaling-apache-giraph-to-a-trillion-edges/10151617006153920>`_ .
Some memory optimizations to try are:

*   Enable ByteArrayPartition store that Facebook implemented to optimize
    memory usage in Giraph using the option
    **-Dgiraph.partitionClass=org.apache.giraph.partition.ByteArrayPartition**.
    This option significantly decreases the amount of memory needed to run
    Giraph jobs.
    Giraph's default partition store is the SimplePartitionStore (which stores
    the graph as Java objects).
    The ByteArrayPartition store keeps only a representative vertex that
    serializes/deserializes on the fly thereby saving memory.
    Be careful to use this option, because some algorithms may fail with this
    option on.
*   If messages are stored as complex objects, try enabling the option
    **-Dgiraph.useMessageSizeEncoding=true**.
    *Note: This option will cause jobs to crash if the messages are simple
    objects.*

Other Giraph options are available at http://giraph.apache.org/options.html.
These options can be set using -D<option=value> in the command for running the
Hadoop job.
For large clusters, "The Hadoop Real World Solutions Cookbook, Owens et al.
2013" recommends controlling the number of messaging threads used by each
worker to communicate with other workers, and increasing the maximum number of
messages flushed in bulk.

