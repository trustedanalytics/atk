/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.plugins.graphconversions

import org.apache.spark.graphx.{ Edge => GraphXEdge }
import org.trustedanalytics.atk.graphbuilder.elements.GBEdge

object GraphConversions {

  /**
   * Converts GraphBuilder edges (ATK internal representation) into GraphX edges.
   *
   * @param gbEdge Incoming ATK edge to be converted into a GraphX edge.
   * @param canonicalOrientation If true, edges are placed in a canonical orientation in which src < dst.
   * @return GraphX representation of the incoming edge.
   */
  def createGraphXEdgeFromGBEdge(gbEdge: GBEdge, canonicalOrientation: Boolean = false): GraphXEdge[Long] = {

    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]

    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]

    if (canonicalOrientation && srcId > destId)
      GraphXEdge[Long](destId, srcId)
    else
      GraphXEdge[Long](srcId, destId)
  }
}
