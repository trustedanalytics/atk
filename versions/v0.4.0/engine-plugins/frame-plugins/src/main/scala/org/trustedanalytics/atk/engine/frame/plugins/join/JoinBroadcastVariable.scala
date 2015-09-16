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

package org.trustedanalytics.atk.engine.frame.plugins.join

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

import scala.collection.mutable.{ HashMap, MultiMap, Set }
import scala.util.{ Try, Random }

/**
 * Broadcast variable for joins
 *
 * The broadcast variable is represented as a sequence of multi-maps. Multi-maps allow us to
 * support duplicate keys during a join. The key in the multi-map is the join key, and the value is row.
 * Representing the broadcast variable as a sequence of multi-maps allows us to support broadcast variables
 * larger than 2GB (current limit in Spark 1.2).
 *
 * @param joinParam Join parameter for data frame
 */
case class JoinBroadcastVariable(joinParam: RddJoinParam) {
  require(joinParam != null, "Join parameter should not be null")

  // Represented as a sequence of multi-maps to support broadcast variables larger than 2GB
  // Using multi-maps instead of hash maps so that we can support duplicate keys.
  val broadcastMultiMaps: Seq[Broadcast[MultiMap[Any, Row]]] = createBroadcastMultiMaps(joinParam)

  /**
   * Get matching set of rows by key from broadcast join variable
   *
   * @param key Join key
   * @return Matching set of rows if found. Multiple rows might match if there are duplicate keys.
   */
  def get(key: Any): Option[Set[Row]] = {
    var rowOption: Option[Set[Row]] = None
    var i = 0
    val numMultiMaps = length()

    do {
      rowOption = broadcastMultiMaps(i).value.get(key)
      i = i + 1
    } while (i < numMultiMaps && rowOption.isEmpty)

    rowOption
  }

  /**
   * Get length of broadcast variable
   *
   * @return length of sequence of broadcast multi-maps
   */
  def length(): Int = broadcastMultiMaps.size

  // Create the broadcast variable for the join
  private def createBroadcastMultiMaps(joinParam: RddJoinParam): Seq[Broadcast[MultiMap[Any, Row]]] = {
    //Grouping by key to ensure that duplicate keys are not split across different broadcast variables
    val broadcastList = joinParam.frame.groupByRows(row => row.value(joinParam.joinColumn)).collect().toList

    val rddSizeInBytes = joinParam.estimatedSizeInBytes.getOrElse(Long.MaxValue)
    val numBroadcastVars = if (broadcastList.nonEmpty && rddSizeInBytes < Long.MaxValue && rddSizeInBytes > 0) {
      Math.ceil(rddSizeInBytes.toDouble / Int.MaxValue).toInt // Limit size of each broadcast var to 2G (MaxInt)
    }
    else 1

    val broadcastMultiMaps = listToMultiMap(broadcastList, numBroadcastVars)
    broadcastMultiMaps.map(map => joinParam.frame.sparkContext.broadcast(map))
  }

  //Converts list to sequence of multi-maps by randomly assigning list elements to multi-maps.
  //Broadcast variables are stored as multi-maps to ensure results are not lost when RDD has duplicate keys
  private def listToMultiMap(list: List[(Any, Iterable[Row])], numMultiMaps: Int): Seq[MultiMap[Any, Row]] = {
    require(numMultiMaps > 0, "Size of multimap should exceed zero")
    val random = new Random(0) //Using seed to get deterministic results
    val multiMaps = (0 until numMultiMaps).map(_ => new HashMap[Any, Set[Row]] with MultiMap[Any, Row]).toSeq

    list.foldLeft(multiMaps) {
      case (maps, (key, rows)) =>
        val i = random.nextInt(numMultiMaps) //randomly split into multiple multi-maps
        rows.foreach(row => maps(i).addBinding(key, row))
        maps
    }
  }
}
