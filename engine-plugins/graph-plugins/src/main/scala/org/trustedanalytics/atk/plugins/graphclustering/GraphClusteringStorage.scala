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

import java.io.Serializable

import org.trustedanalytics.atk.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanSchemaWriter
import org.trustedanalytics.atk.domain.schema.GraphSchema
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }

case class GraphClusteringStorage(titanStorage: TitanGraph)
    extends GraphClusteringStorageInterface {

  override def addSchema(): Unit = {

    val schema = new GraphSchema(
      List(EdgeLabelDef(GraphClusteringConstants.LabelPropertyValue)),
      List(PropertyDef(PropertyType.Vertex, GraphSchema.labelProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexNodeCountProperty, classOf[Long]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexNodeNameProperty, classOf[String]),
        PropertyDef(PropertyType.Vertex, GraphClusteringConstants.VertexIterationProperty, classOf[Int]))
    )
    val schemaWriter = new TitanSchemaWriter(titanStorage)

    schemaWriter.write(schema)
  }

  override def addVertexAndEdges(src: Long, dest: Long, count: Long, name: String, iteration: Int): Long = {

    val metaNodeVertex = addVertex(count, name, iteration)
    addEdge(metaNodeVertex, src)
    addEdge(metaNodeVertex, dest)

    metaNodeVertex.getId.asInstanceOf[Long]
  }

  override def commit(): Unit = {
    titanStorage.commit()
  }

  override def shutdown(): Unit = {
    titanStorage.shutdown()
  }

  private def addVertex(vertexCount: Long, vertexName: String, iteration: Int): Vertex = {

    val vertex = titanStorage.addVertex(null)
    vertex.setProperty(GraphSchema.labelProperty, GraphClusteringConstants.LabelPropertyValue)
    vertex.setProperty(GraphSchema.labelProperty, vertexCount)

    // TODO: this is testing only, remove later.
    vertex.setProperty(GraphClusteringConstants.VertexNodeNameProperty, vertexName)
    vertex.setProperty(GraphClusteringConstants.VertexIterationProperty, iteration)

    vertex
  }

  private def addEdge(src: Vertex, dest: Long): Edge = {
    titanStorage.addEdge(null, src, titanStorage.getVertex(dest), GraphClusteringConstants.LabelPropertyValue)
  }

}
