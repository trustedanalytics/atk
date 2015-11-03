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

package org.trustedanalytics.atk.plugins.testutils

import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }

/**
 * Provides methods for comparing vertices and sets of vertices when approximate equality is acceptable in a
 * list of specified properties. Properties that can be approximately equal must have values that are of one of the
 * types Float, Double, Vector[Double] or Vector[Float] valued
 */
object ApproximateVertexEquality {

  /**
   * Test two vertices for approximate equality.
   * @param v1 First vertex.
   * @param v2 Second vertex.
   * @param namesOfApproximateProperties List of properties that are allowed to be approximately equal.
   * @param threshold Threshold for floating point comparisons when performing approximate comparisons of properties.
   * @return True if the two vertices have the same ids, they are exactly equal off from the list of
   *         approximate properties, and they are approximately equal on the list of properties specified.
   */
  def approximatelyEquals(v1: GBVertex, v2: GBVertex, namesOfApproximateProperties: List[String], threshold: Double): Boolean = {

    val properties1 = v1.fullProperties
    val properties2 = v2.fullProperties

    val keys1 = properties1.map({ case p: Property => p.key })
    val keys2 = properties2.map({ case p: Property => p.key })

    v1.physicalId == v2.physicalId &&
      v1.gbId.equals(v2.gbId) &&
      keys1.equals(keys2) &&
      keys1.forall(k => (namesOfApproximateProperties.contains(k) &&
        propertiesApproximatelyEqual(v1.getProperty(k), v2.getProperty(k), threshold)) ||
        (v1.getProperty(k) equals v2.getProperty(k)))
  }

  /**
   * Test two sets of vertices for approximate equality.
   *
   * The equality check across sets is by brute force...  this is slower than hashing would be but
   * these methods are meant to be run on small sets when evaluating unit tests. Change it up if you have
   * a performance problem here.
   *
   * @param vertexSet1 First set of vertices.
   * @param vertexSet2 Second set of vertices.
   * @param namesOfApproximateProperties List of properties that are allowed to be approximately equal.
   * @param threshold Threshold for floating point comparisons when performing approximate comparisons of properties.
   * @return True if the two sets are the same size and for every vertex in the first set, there is a vertex in the
   *         second set with which it is approximately equal.
   */
  def approximatelyEquals(vertexSet1: Set[GBVertex],
                          vertexSet2: Set[GBVertex],
                          namesOfApproximateProperties: List[String],
                          threshold: Double): Boolean = {

    (vertexSet1.size == vertexSet2.size) &&
      vertexSet1.forall(v => vertexSet2.exists(u => approximatelyEquals(u, v, namesOfApproximateProperties, threshold)))
  }

  /*
   * Tests if two property options are approximately equal given a threshold.
   *
   * If both options are empty, true is returned.
   * If one option is empty but the other is not, false is returned.
   * If the two property values are not of the same type, false is returned.
   * If the property values are not Float, Double, Vector[Float] or Vector[Double], the result is false.
   * Otherwise, the two values are considered equal if their l1 distance is below the threshold.
   *
   * @param propertyOption1 First option for a property.
   * @param propertyOption2 Second option for a property.
   * @param threshold Threshold of comparision; if |x - y| < threshold then x and y are considered approximately
   *                  equal.
   * @return Results of the approximate comparison test for the property options.
   */
  private def propertiesApproximatelyEqual(propertyOption1: Option[Property],
                                           propertyOption2: Option[Property],
                                           threshold: Double): Boolean = {

    if (propertyOption1.isEmpty != propertyOption2.isEmpty) {
      false
    }
    else if (propertyOption1.isEmpty && propertyOption2.isEmpty) {
      true
    }
    else {
      (propertyOption1.get.value, propertyOption2.get.value) match {
        case (v1: Float, v2: Float) => Math.abs(v1 - v2) < threshold
        case (v1: Double, v2: Double) => Math.abs(v1 - v2) < threshold
        case (v1: Vector[_], v2: Vector[_]) => (v1.length == v2.length) &&
          (VectorMath.l1Distance(v1.asInstanceOf[Vector[Double]], v2.asInstanceOf[Vector[Double]]) < threshold)
        case _ => false
      }
    }
  }

}
