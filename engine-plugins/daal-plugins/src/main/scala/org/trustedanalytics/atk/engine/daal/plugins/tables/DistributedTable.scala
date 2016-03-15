package org.trustedanalytics.atk.engine.daal.plugins.tables

import org.apache.spark.rdd.RDD

/**
 * Abstract class for RDD of DAAL numeric tables
 */
abstract class DistributedTable[T](tableRdd: RDD[T], numRows: Long) extends Serializable {
  /**
   * Get RDD of indexed numeric table
   */
  def rdd: RDD[T] = tableRdd

  /**
   * Cache distributed table in memory.
   */
  def cache(): Unit = {
    tableRdd.cache()
  }

  /**
   * Unpersist cached distributed table.
   */
  def unpersist(): Unit = {
    tableRdd.unpersist()
  }
}
