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

import org.trustedanalytics.atk.engine.EngineConfig
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
 * Limits the number of partitions for an RDD based on the available spark cores.
 *
 * Some plugins (e.g., group-by) perform poorly if there are too many partitions relative to the
 * number of available Spark cores. This is more pronounced in the reduce phase.
 */
object SparkCoresPartitioner extends Serializable {

  /**
   * Get the number number of partitions based on available spark cores
   */
  def getNumPartitions[T](rdd: RDD[T]): Int = {
    val maxPartitions = getMaxSparkPartitions(rdd)
    val numPartitions = Math.min(rdd.partitions.length, maxPartitions)

    //TODO: Replace print statement with IAT event context when event contexts are supported at workers
    println(s"Number of partitions computed by SparkCoresPartitioner: $numPartitions")
    numPartitions
  }

  // Get the maximum number of spark partitions to run
  private def getMaxSparkPartitions[T](rdd: RDD[T]): Int = {
    if (EngineConfig.isSparkOnYarn) {
      getMaxPartitionsYarn(rdd)
    }
    else {
      getMaxPartitionsStandalone(rdd)
    }
  }

  // Get the maximum number of partitions for Spark Yarn
  private def getMaxPartitionsYarn[T](rdd: RDD[T]): Int = {
    val numExecutors = Math.max(1, rdd.sparkContext.getExecutorStorageStatus.size - 1)
    val coresPerExecutor = Try({
      EngineConfig.sparkConfProperties("spark.executor.cores").toInt
    }).getOrElse(1)
    numExecutors * coresPerExecutor * EngineConfig.maxTasksPerCore
  }

  // Get the maximum number of partitions for Spark stand-alone
  private def getMaxPartitionsStandalone[T](rdd: RDD[T]): Int = {
    val numExecutors = Math.max(1, rdd.sparkContext.getExecutorStorageStatus.size - 1)
    val numSparkCores = {
      val maxSparkCores = rdd.sparkContext.getConf.getInt("spark.cores.max", 0)
      if (maxSparkCores > 0) maxSparkCores else Runtime.getRuntime.availableProcessors() * numExecutors
    }
    numSparkCores * EngineConfig.maxTasksPerCore
  }
}
