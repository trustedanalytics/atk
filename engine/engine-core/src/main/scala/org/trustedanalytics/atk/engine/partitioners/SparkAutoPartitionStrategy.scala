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

package org.trustedanalytics.atk.engine.partitioners

/**
 * Partitioning Strategies for Spark RDDs.
 *
 * Uses Spark's coalesce() to re-partition RDDs.
 *
 * @see org.apache.spark.rdd.RDD
 */
object SparkAutoPartitionStrategy {

  /**
   * Partitioning Strategies for Spark RDDs
   * @param name Partition strategy name
   */
  sealed abstract class PartitionStrategy(val name: String) {
    override def toString = name
  }

  /**
   * Disable repartitioning of Spark RDDs.
   */
  case object Disabled extends PartitionStrategy("DISABLED")

  /**
   * Repartition RDDs only when the requested number of partitions is less than the current partitions.
   *
   * Shrinking RDD partitions is less expensive and does not involve a shuffle operation.
   *
   * @see org.apache.spark.rdd.RDD#coalesce(Int, Boolean)
   */
  case object ShrinkOnly extends PartitionStrategy("SHRINK_ONLY")

  /**
   * Repartition RDDs only when the requested number of partitions is less or greater than the current partitions.
   *
   * Uses more-expensive Spark shuffle operation to shrink or grow partitions.
   *
   * @see org.apache.spark.rdd.RDD#coalesce(Int, Boolean)
   */
  case object ShrinkOrGrow extends PartitionStrategy("SHRINK_OR_GROW")

  val partitionStrategies: Seq[PartitionStrategy] = Seq(Disabled, ShrinkOnly, ShrinkOrGrow)

  /**
   * Find mode by name
   *
   * @param name Name of mode
   * @return Matching mode. If not found, defaults to Disabled
   */
  def getRepartitionStrategy(name: String): PartitionStrategy = {
    val strategy = partitionStrategies.find(m => m.name.equalsIgnoreCase(name)).getOrElse({
      throw new IllegalArgumentException(s"Unsupported Spark auto-partitioning strategy: $name")
    })
    strategy
  }
}
