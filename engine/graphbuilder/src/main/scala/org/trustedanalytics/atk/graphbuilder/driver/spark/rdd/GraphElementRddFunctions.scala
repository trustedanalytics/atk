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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GraphElement }
import org.trustedanalytics.atk.graphbuilder.elements._
import org.apache.spark.rdd.RDD

/**
 * Functions applicable to RDD's of GraphElements
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 *
 * @param self input that these functions are applicable to
 */
class GraphElementRddFunctions(self: RDD[GraphElement]) {

  /**
   * Get all of the Edges from an RDD made up of both Edges and Vertices.
   */
  def filterEdges(): RDD[GBEdge] = {
    self.flatMap {
      case e: GBEdge => Some(e)
      case _ => None
    }
  }

  /**
   * Get all of the Vertices from an RDD made up of both Edges and Vertices.
   */
  def filterVertices(): RDD[GBVertex] = {
    self.flatMap {
      case v: GBVertex => Some(v)
      case _ => None
    }
  }
}
