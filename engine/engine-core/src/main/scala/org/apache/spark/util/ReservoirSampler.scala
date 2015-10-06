/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.spark.util

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

import scala.reflect.ClassTag

/**
 * Sampled keys in each RDD partition
 *
 * @param partitionId Partition ID
 * @param partitionLength Number of rows in partition
 * @param samples Array of sampled keys
 */
case class PartitionSample[K](partitionId: Int,
                              partitionLength: Int,
                              samples: Array[K])

/**
 * Reservoir sampler samples k rows from an RDD.
 *
 * The reservoir sample first creates a reservoir array with k items. Then it iterates
 * through the rest of the items and replaces items in the array by generating a
 * random number r between 1 and i at the ith element. If the random number is less than k,
 * then the rth element in the array is replaced by the ith element
 */
object ReservoirSampler {
  val DEFAULT_PARTITION_SAMPLES = 100
  val MAX_SAMPLES = 1000000

  /**
   * Reservoir sampling implementation that also returns the input size.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total row count, an array of partition samples)
   */
  def sampleAndCount[K: ClassTag](
    rdd: RDD[K],
    sampleSizePerPartition: Int): (Long, Array[PartitionSample[K]]) = {
    val (rowCount, sampleArray) = RangePartitioner.sketch(rdd, sampleSizePerPartition)
    val partitionSamples = sampleArray.map {
      case (partitionId, partitionLength, samples) =>
        PartitionSample(partitionId, partitionLength, samples)
    }
    (rowCount, partitionSamples)
  }

  /**
   * Create an XORShift random number generator
   *
   * @see org.apache.spark.util.random.XORShiftRandom
   * @param seed Seed for random number generator
   * @return XORShift random number generator
   */
  def createXORShiftRandom(seed: Long): XORShiftRandom = new XORShiftRandom(seed)

  /**
   * Get count of sampled keys
   * @return (total row count, Map of keys and sample count)
   */
  def sampleAndCountKeys[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): (Long, Map[K, Int]) = {
    val samplesPerPartition = getPartitionSampleSize(rdd)
    val (rowCount, sampledKeys) = ReservoirSampler.sampleAndCount(rdd.map(_._1), samplesPerPartition)
    var keyCounts = Map[K, Int]()
    sampledKeys.foreach {
      case (p) =>
        for (k <- p.samples) {
          val count = keyCounts.getOrElse(k, 0) + 1
          keyCounts += (k -> count)
        }
    }
    (rowCount, keyCounts)
  }

  /**
   * Get the number of samples per partition
   *
   * Calculates the number of samples per partiton so that the total
   * number of samples does not exceed the maximum sample size.
   *
   * @param rdd RDD to sample
   * @param samplesPerPartition Number of samples per partition
   * @param maxSamples Maximum number of samples
   * @return Samples per partition
   */
  def getPartitionSampleSize[K, V](rdd: RDD[(K, V)],
                                   samplesPerPartition: Int = DEFAULT_PARTITION_SAMPLES,
                                   maxSamples: Int = MAX_SAMPLES): Int = {
    val numPartitions = rdd.partitions.length
    val sampleSize = math.min(samplesPerPartition * numPartitions, maxSamples)
    math.ceil(sampleSize / numPartitions).toInt
  }
}
