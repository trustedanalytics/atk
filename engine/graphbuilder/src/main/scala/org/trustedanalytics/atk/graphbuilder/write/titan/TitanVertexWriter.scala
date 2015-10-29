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


package org.trustedanalytics.atk.graphbuilder.write.titan

import org.trustedanalytics.atk.graphbuilder.elements.GbIdToPhysicalId
import org.trustedanalytics.atk.graphbuilder.write.VertexWriter
import org.trustedanalytics.atk.graphbuilder.elements.{ GbIdToPhysicalId, GBVertex }
import org.trustedanalytics.atk.graphbuilder.write.VertexWriter
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanIdUtils.titanId

/**
 * Wraps a blueprints VertexWriter to add some Titan specific functionality that is needed
 *
 * @param vertexWriter blueprints VertexWriter
 */
class TitanVertexWriter(vertexWriter: VertexWriter) extends Serializable {

  /**
   * Write a vertex returning the GbIdToPhysicalId mapping so that we can match up Physical ids to Edges
   */
  def write(vertex: GBVertex): GbIdToPhysicalId = {
    val bpVertex = vertexWriter.write(vertex)
    new GbIdToPhysicalId(vertex.gbId, titanId(bpVertex).asInstanceOf[AnyRef])
  }
}
