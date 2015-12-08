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

package org.trustedanalytics.atk.spray.json

/**
 * Methods to help with JSON serialization of maps
 */
object JsonMaps {

  /**
   * Converts a map with String keys to a map with Int key
   *
   * JSON spec requires String keys and spray-json can't handle the conversion;
   * Plugins requiring a map parameter with non-string keys must defined them as Map[String, ValueT]
   * and convert them here.
   *
   * (Adding explicit implicit conversion to Map[Int, Int], etc. in DomainJsonProtocol didn't seem to take)
   *
   * @param m map to convert
   * @tparam V value type in the map
   * @return converted map
   */
  def convertMapKeyToInt[V](m: Map[String, V]): Map[Int, V] = {
    m.map { case (k, v) => (k.toInt, v) }
  }

  /**
   * Extracts a map from an Option converting String keys to Int keys
   *
   * @param m Option of map to convert
   * @param default value if map is empty
   * @tparam V value type in the map
   * @return converted map or default
   */
  def convertMapKeyToInt[V](m: Option[Map[String, V]], default: => Map[Int, V]): Map[Int, V] = {
    if (m.isEmpty) default else convertMapKeyToInt[V](m.get)
  }

}
