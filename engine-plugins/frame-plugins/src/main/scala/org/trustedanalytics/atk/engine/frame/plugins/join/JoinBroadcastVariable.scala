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

package org.trustedanalytics.atk.engine.frame.plugins.join

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
  val broadcastMultiMap: Broadcast[MultiMap[Any, Row]] = createBroadcastMultiMaps(joinParam)

  /**
   * Get matching set of rows by key from broadcast join variable
   *
   * @param key Join key
   * @return Matching set of rows if found. Multiple rows might match if there are duplicate keys.
   */
  def get(key: Any): Option[Set[Row]] = {
    broadcastMultiMap.value.get(key)
  }

  // Create the broadcast variable for the join
  private def createBroadcastMultiMaps(joinParam: RddJoinParam): Broadcast[MultiMap[Any, Row]] = {
    //Grouping by key to ensure that duplicate keys are not split across different broadcast variables
    val broadcastList = joinParam.frame.groupByRows(row => row.values(joinParam.joinColumns.toList)).collect().toList
    val broadcastMultiMap = listToMultiMap(broadcastList)
    joinParam.frame.sparkContext.broadcast(broadcastMultiMap)
  }

  //Broadcast variables are stored as multi-maps to ensure results are not lost when RDD has duplicate keys
  private def listToMultiMap(list: List[(Any, Iterable[Row])]): MultiMap[Any, Row] = {
    val multiMap = new HashMap[Any, Set[Row]] with MultiMap[Any, Row]

    list.foldLeft(multiMap) {
      case (map, (key, rows)) =>
        rows.foreach(row => map.addBinding(key, row))
        map
    }
  }
}
