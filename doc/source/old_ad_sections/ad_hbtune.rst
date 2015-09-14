.. _old_ad_sections/ad_hbtune:

.. _ad_hbtune:

.. index::
    single: HBase
    single: performance
    single: tuning

HBase Performance Tuning
========================

Two comprehensive sources for performance tuning in HBase are:

*   `HBase reference guide <http://hbase.apache.org/book.html#important_configurations>`__

*   `The HBase Administration Cookbook`_

Some of the recommendations for production clusters are detailed below.
Remember to sync these changes to all servers in the cluster.

1.  Increase ulimit settings.
    The default Linux setting for maximum number of file descriptors for a user
    is 1024 which is too low.
    The number of file descriptors (nofile) and open processes for the user
    running the Hadoop/HBase daemons should be set to a minimum of 32768,
    otherwise the system will experience problems under heavy load.
    Instructions for changing */etc/security/limits.conf* are available here:
    http://hbase.apache.org/book.html#ulimit
#.  Garbage collection.
    Pauses due to garbage collection can impact performance.
    For latency-sensitive applications, Cloudera recommends
    **-XX:+UseParNewGC** and **-XX:+UseConcMarkSweepGC** in *hbase-env.sh*.
    The MemStore-Local Allocation buffers (MSLAB) which reduce heap
    fragmentation under heavy write-load are enabled by default in HBase.
    The MSLAB setting is **hbase.hregion.memstore.mslab.enabled** in
    *hbase-site.xml*.
    http://blog.cloudera.com/blog/2011/02/avoiding-full-gcs-in-hbase-with-memstore-local-allocation-buffers-part-1

#.  Enable column family compression.
    Compression boosts performance by reducing the size of store files thereby
    reducing I/O http://hbase.apache.org/book.html#compression.
    The Snappy library from Google is faster than LZO and easier to install.
    Instructions for installing Snappy are available at:
    http://hbase.apache.org/book.html#d0e24641
    Once installed, turn on column family compression when creating the table,
    for example::

      create 'mytable', {NAME => 'mycolumnfamily1', COMPRESSION => 'SNAPPY'}

#.  Avoid update blocking for write-heavy workloads.
    If the cluster has enough memory, increase
    **hbase.hregion.memstore.block.multiplier** and
    **hbase.hstore.blockingStoreFiles** in *hbase-site.xml* to avoid blocking
    in write-heavy workloads.
    This setting needs to be tuned carefully because it increases the number of
    files to compact.
    See `The HBase Administration Cookbook`_ (Chapter 9)

#.  Increase region server handler count.
    If the workload has many concurrent clients, increasing the number of
    handlers can improve performance.
    However, too many handlers will lead to out of memory errors.
    See `The HBase Administration Cookbook <http://library.intel.com/Catalog/CatalogItemDetails.aspx?id=225983>`__ (Chapter 9)

Other recommendations include turning off automatic compactions and doing it
manually: http://hbase.apache.org/book.html#managed.compactions.

.. _The HBase Administration Cookbook: http://library.intel.com/Catalog/CatalogItemDetails.aspx?id=225983
