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

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.partitioners.SparkAutoPartitionStrategy.{ ShrinkOnly, ShrinkOrGrow }
import org.trustedanalytics.atk.engine.{ FileStorage, EngineConfig }
import org.apache.spark.frame.FrameRdd

/**
 * Calculate a best guess for the number of partitions that should be used for loading this file into a Spark RDD.
 *
 * This number won't be perfect but should be better than using default.
 */
class SparkAutoPartitioner(fileStorage: FileStorage) extends EventLogging with EventLoggingImplicits {

  /**
   * Calculate a best guess for the minimum number of partitions that should be used for loading this file into a Spark RDD.
   *
   * This number won't be perfect but should be better than using default.
   *
   * @param path relative path
   * @return number of partitions that should be used for loading this file into a Spark RDD
   */
  def partitionsForFile(path: String)(implicit invocation: Invocation): Int = withContext[Int]("spark-auto-partioning") {
    //General Advice on Paritioning:
    // - Choose a reasonable number of partitions: no smaller than 100, no larger than 10,000 (large cluster)
    // - Lower bound: at least 2x number of cores in your cluster
    // - Upper bound: ensure your tasks take at least 100ms (if they are going faster, then you are probably spending more
    //   time scheduling tasks than executing them)
    // - Generally better to have slightly too many partitions than too few
    val size = fileStorage.size(path)
    val partitions = partitionsFromFileSize(size)
    info("auto partitioning path:" + path + ", size:" + size + ", partitions:" + partitions)
    partitions
  }

  /**
   * Repartition RDD based on configured Spark auto-partitioning strategy
   *
   * @param path relative path
   * @param frameRdd  frame RDD
   * @return repartitioned frame RDD
   */
  def repartitionFromFileSize(path: String, frameRdd: FrameRdd)(implicit invocation: Invocation): FrameRdd = withContext[FrameRdd]("spark-auto-partitioning") {
    val repartitionedRdd = EngineConfig.repartitionStrategy match {
      case ShrinkOnly =>
        repartition(path, frameRdd, shuffle = false)
      case ShrinkOrGrow =>
        repartition(path, frameRdd, shuffle = true)
      case _ => frameRdd
    }
    info("re-partitioning path:" + path + ", from " + frameRdd.partitions.length + " to " + repartitionedRdd.partitions.length + " partitions")
    repartitionedRdd
  }

  /**
   * Repartition RDD using requested number of partitions.
   *
   * Uses Spark's coalesce() to re-partition RDDs.
   *
   * @param frameRdd frame RDD
   * @param shuffle If true, RDD partitions can increase or decrease, else if false, RDD partitions can only decrease
   * @return repartitioned frame RDD
   */
  private[engine] def repartition(path: String,
                                  frameRdd: FrameRdd,
                                  shuffle: Boolean = false): FrameRdd = {
    val framePartitions = frameRdd.partitions.length

    // Frame compression ratio prevents us from under-estimating actual file size for compressed formats like Parquet
    val approxFileSize = fileStorage.size(path) * EngineConfig.frameCompressionRatio
    val desiredPartitions = partitionsFromFileSize(approxFileSize.toLong)

    val delta = Math.abs(desiredPartitions - framePartitions) / framePartitions.toDouble
    if (delta >= EngineConfig.repartitionThreshold) {
      val repartitionedRdd = frameRdd.coalesce(desiredPartitions, shuffle)
      new FrameRdd(frameRdd.frameSchema, repartitionedRdd)
    }
    else {
      frameRdd
    }
  }

  /**
   * Get the partition count given a file size,
   * @param fileSize size of file in bytes
   * @return partition count that should be used
   */
  private[engine] def partitionsFromFileSize(fileSize: Long): Int = {
    var partitionCount = EngineConfig.maxPartitions
    EngineConfig.autoPartitionerConfig.foreach(partitionConfig => {
      if (fileSize <= partitionConfig.fileSizeUpperBound) {
        partitionCount = partitionConfig.partitionCount
      }
      else {
        // list is sorted, so we can exit early
        return partitionCount
      }
    })
    partitionCount
  }
}

/**
 * Map upper bounds of file size to partition sizes
 * @param fileSizeUpperBound upper bound on file size for the partitionCount in bytes
 * @param partitionCount number of partitions to use
 */
case class FileSizeToPartitionSize(fileSizeUpperBound: Long, partitionCount: Int)
