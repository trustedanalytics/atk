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

package org.trustedanalytics.atk.plugins.graphclustering

import org.trustedanalytics.atk.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import org.trustedanalytics.atk.domain.schema.GraphSchema

case class GraphClusteringStorage(storage: Any)
    extends GraphClusteringStorageInterface {

  override def addSchema(): Unit = {

    val schema = new GraphSchema(
      List(EdgeLabelDef(GraphClusteringConstants.LabelPropertyValue)),
      List(PropertyDef(PropertyType.Vertex, GraphSchema.labelProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexNodeCountProperty, classOf[Long]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexNodeNameProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexIterationProperty, classOf[Int]))
    )
    // TODO: write to a graph database
  }

  override def addVertexAndEdges(src: Long, dest: Long, count: Long, name: String, iteration: Int): Long = {
    0
  }

  override def commit(): Unit = {
  }

  override def shutdown(): Unit = {
  }

}
