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

import org.apache.spark.rdd.RDD

/**
 * Utility function for querying status of Spark RDD
 */
object SparkRddStatus {
  val SHUFFLE_MEMORY_FRACTION = 0.2
  val SHUFFLE_SAFETY_FRACTION = 0.8

  /** Get number of Spark executors from RDD's Spark context */
  def getExecutorCount[T](rdd: RDD[T]): Int = {
    Math.max(1, rdd.sparkContext.getExecutorStorageStatus.size - 1)
  }

  /** Get the total number of Spark cores available for executing the RDD */
  def getTotalSparkCores[T](rdd: RDD[T], isSparkOnYarn: Boolean): Int = {
    val sparkConf = rdd.sparkContext.getConf

    if (isSparkOnYarn) {
      sparkConf.getInt("spark.executor.cores", 1) * getExecutorCount(rdd)
    }
    else {
      sparkConf.getOption("spark.cores.max") match {
        case s: Some[String] if s.get != Int.MaxValue.toString => s.get.toInt
        case _ => Runtime.getRuntime.availableProcessors() * getExecutorCount(rdd)
      }
    }
  }

  /** Get the Spark executor memory in bytes */
  def getExecutorMemory[T](rdd: RDD[T]): Long = {
    val executorMemoryMb = rdd.sparkContext.executorMemory.toLong
    executorMemoryMb * 1024 * 1024
  }

  /** Get the Spark executor memory reserved for the shuffle in bytes */
  def getExecutorShuffleMemory[T](rdd: RDD[T]): Long = {
    val conf = rdd.sparkContext.getConf
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", SHUFFLE_MEMORY_FRACTION)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", SHUFFLE_SAFETY_FRACTION)
    (getExecutorMemory(rdd) * memoryFraction * safetyFraction).toLong
  }
}
