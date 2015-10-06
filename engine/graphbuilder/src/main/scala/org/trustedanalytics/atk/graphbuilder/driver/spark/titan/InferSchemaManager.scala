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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.schema.InferSchemaFromData
import org.trustedanalytics.atk.graphbuilder.elements.{ GraphElement, GBEdge, GBVertex }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.parser.rule.DataTypeResolver
import org.trustedanalytics.atk.graphbuilder.schema.{ SchemaAccumulableParam, GraphSchema, InferSchemaFromData, InferSchemaFromRules }
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanSchemaWriter
import org.apache.spark.Accumulable
import org.apache.spark.rdd.RDD

/**
 * Infers schema and writes as appropriate
 */
class InferSchemaManager(config: GraphBuilderConfig) extends Serializable {

  val titanConnector = new TitanGraphConnector(config.titanConfig)
  val dataTypeResolver = new DataTypeResolver(config.inputSchema)
  val inferSchemaFromRules = new InferSchemaFromRules(dataTypeResolver, config.vertexRules, config.edgeRules)
  val inferSchemaFromData = new InferSchemaFromData()

  /**
   * True if the entire schema was not infer-able from the rules
   */
  def needsToInferSchemaFromData: Boolean = {
    !inferSchemaFromRules.canInferAll
  }

  /**
   * Write the Inferred Schema to Titan, as much as possible.
   */
  def writeSchemaFromRules() = {
    writeSchema(inferSchemaFromRules.inferGraphSchema())
  }

  /**
   * Infer the schema by passing over each edge and vertex.
   */
  def writeSchemaFromData(edges: RDD[GBEdge], vertices: RDD[GBVertex]) = {
    val accum = new Accumulable[InferSchemaFromData, GraphElement](new InferSchemaFromData, new SchemaAccumulableParam)
    edges.foreach(edge => accum.add(edge))
    vertices.foreach(vertex => accum.add(vertex))
    writeSchema(accum.value.graphSchema)
  }

  /**
   * Write the supplied schema to Titan
   */
  def writeSchema(graphSchema: GraphSchema) = {
    val graph = titanConnector.connect()
    try {
      println("Writing the schema to Titan: " + graphSchema.count + " items")
      val writer = new TitanSchemaWriter(graph)
      writer.write(graphSchema)
      graph.commit()
    }
    finally {
      graph.shutdown()
    }
  }
}
