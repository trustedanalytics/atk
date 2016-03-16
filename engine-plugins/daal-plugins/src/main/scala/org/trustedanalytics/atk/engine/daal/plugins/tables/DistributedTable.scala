/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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
