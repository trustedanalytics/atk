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

package org.trustedanalytics.atk.graphbuilder.elements

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.graphbuilder.util.StringUtils

import scala.util.Try

/**
 * A property on a Vertex or Edge.
 *
 * @param key the name of the property
 * @param value the value of the property
 */
case class Property(key: String, value: Any) {

  /**
   * Convenience constructor
   */
  def this(key: Any, value: Any) = {
    this(StringUtils.nullSafeToString(key), value)
  }
}

object Property {

  /**
   * Merge two set of properties so that keys appear once.
   *
   * Conflicts are handled arbitrarily.
   */
  def merge(setA: Set[Property], setB: Set[Property]): Set[Property] = {
    val unionPotentialKeyConflicts = setA ++ setB
    val mapWithoutDuplicates = unionPotentialKeyConflicts.map(p => (p.key, p)).toMap
    mapWithoutDuplicates.valuesIterator.toSet
  }

}

/**
 * An ordering of properties by key and value
 *
 * Can be used to enable Spark's sort-based shuffle which is more memory-efficient.
 */
object PropertyOrdering extends Ordering[Property] {
  def compare(a: Property, b: Property) = {
    val keyComparison = a.key compare b.key
    if (keyComparison == 0) {
      Try(DataTypes.compare(a.value, b.value))
        .getOrElse(a.value.toString() compare b.value.toString)
    }
    else keyComparison
  }
}
