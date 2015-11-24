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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

/**
 * Broadcast variable for graph builder joins
 *
 * @param rdd Pair RDD to broadcast
 *
 */
case class JoinBroadcastVariable[K, V](rdd: RDD[(K, V)]) {
  import JoinBroadcastVariable._
  //TODO: Create a base class for broadcast join variables to avoid code duplication once we move graphbuilder module to engine
  require(rdd != null, "RDD should not be null")

  // Represented as a sequence of multi-maps to support broadcast variables larger than 2GB
  val broadcastMap: Broadcast[Map[K, V]] = createBroadcastMap(rdd)

  /**
   * Get matching value from broadcast join variable using key
   *
   * @param key Join key
   * @return Optional matching value
   */
  def get(key: K): Option[V] = {
    broadcastMap.value.get(key)
  }

  /**
   * Get matching value from broadcast join variable using key
   *
   * Throws exception if key is not present
   *
   * @param key Key
   * @return Matching value
   */
  def apply(key: K): V = get(key).getOrElse(throw new IllegalArgumentException(s"Could not retrieve key ${key}"))

  // Create the broadcast variable for the join
  private def createBroadcastMap(rdd: RDD[(K, V)]): Broadcast[Map[K, V]] = {
    val broadcastList = rdd.collect().toList

    val map = listToMap(broadcastList)
    rdd.sparkContext.broadcast(map)
  }

  //Converts list to sequence of maps by randomly assigning list elements to maps.
  private def listToMap[K, V](list: List[(K, V)]): Map[K, V] = {
    val map = Map[K, V]()
    list.foreach {
      case (key, value) =>
        map += (key -> value)
    }
    map
  }
}

object JoinBroadcastVariable {

  val rddCompressionFactor = 2.5d //Used to estimate actual size of RDD. Might move to config file

  /**
   * Get the estimated size of the RDD
   */
  def getRddSize[T](rdd: RDD[T]): Long = {
    val storageStatus = rdd.sparkContext.getExecutorStorageStatus
    val rddSize = rddCompressionFactor * storageStatus.map(status => status.memUsedByRdd(rdd.id)).sum
    println(s"Estimated rdd size=${rddSize}, executors=${storageStatus.length - 1}")
    rddSize.toLong
  }

  /**
   * Determine whether to use broadcast join for graph builder
   *
   * @param rdd RDD
   * @param broadcastJoinThreshold Use broadcast variable for join if size of one of the data frames is below threshold
   * @return True if size of RDD is below join threshold
   */
  def useBroadcastVariable[T](rdd: RDD[T], broadcastJoinThreshold: Long): Boolean = {
    val rddSize = getRddSize(rdd)

    val useBroadcastVariable = rddSize > 0 && rddSize < broadcastJoinThreshold
    if (useBroadcastVariable) {
      println(s"Using broadcast join: rdd size=${rddSize}, threshold=${broadcastJoinThreshold}")
    }
    else {
      println(s"Using hash join: rdd size=${rddSize}, threshold=${broadcastJoinThreshold}")
    }

    useBroadcastVariable
  }
}
