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

import org.trustedanalytics.atk.graphbuilder.schema.{ PropertyDef, GraphSchema }
import org.trustedanalytics.atk.graphbuilder.util.PrimitiveConverter
import org.trustedanalytics.atk.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import org.trustedanalytics.atk.graphbuilder.write.SchemaWriter
import com.thinkaurelius.titan.core.{ Multiplicity, TitanGraph }
import com.tinkerpop.blueprints._

/**
 * Titan specific implementation of SchemaWriter.
 * <p>
 * This writer will ignore types that are already defined with the same name.
 * </p>
 */
class TitanSchemaWriter(graph: TitanGraph) extends SchemaWriter {

  if (graph == null) {
    throw new IllegalArgumentException("TitanSchemaWriter requires a non-null Graph")
  }
  if (!graph.isOpen) {
    throw new IllegalArgumentException("TitanSchemaWriter requires an open Graph")
  }

  /**
   * Write the schema definition to the underlying Titan graph database
   */
  override def write(schema: GraphSchema): Unit = {
    writePropertyDefs(schema.propertyDefs)
    writeLabelDefs(schema.edgeLabelDefs)
  }

  /**
   * Create a list of property types in Titan
   * @param propertyDefs the definition of a Property
   */
  private def writePropertyDefs(propertyDefs: List[PropertyDef]): Unit = {
    for (propertyDef <- propertyDefs) {
      writePropertyDef(propertyDef)
    }
  }

  /**
   * Create a property type in Titan
   * @param propertyDef the definition of a Property
   */
  private def writePropertyDef(propertyDef: PropertyDef): Unit = {
    val graphManager = graph.getManagementSystem()
    if (!graphManager.containsRelationType(propertyDef.name)) {
      val property = graphManager.makePropertyKey(propertyDef.name).dataType(PrimitiveConverter.primitivesToObjects(propertyDef.dataType)).make()

      if (propertyDef.unique) {
        //uniqueness enforced by indexing
        graphManager.buildIndex(propertyDef.name, indexType(propertyDef.propertyType)).addKey(property).unique().buildCompositeIndex()
      }
      else if (propertyDef.indexed) {
        // TODO: future: should we implement INDEX_NAME? Using property name as index name for now.
        graphManager.buildIndex(propertyDef.name, indexType(propertyDef.propertyType)).addKey(property).buildCompositeIndex()
      }
    }
    graphManager.commit()
  }

  /**
   * Determine the index type from the supplied PropertyType
   * @param propertyType enumeration to specify Vertex or Edge
   */
  private[titan] def indexType(propertyType: PropertyType.Value): Class[_ <: Element] = {
    if (propertyType == PropertyType.Vertex) {
      classOf[Vertex] // TODO: this should probably be an Index Type property?
    }
    else if (propertyType == PropertyType.Edge) {
      classOf[Edge] // TODO: this should probably be an Index Type property?
    }
    else {
      throw new RuntimeException("Unknown PropertyType is not yet implemented: " + propertyType)
    }
  }

  /**
   * Create the edge label definitions in Titan
   * @param edgeLabelDefs the edge labels needed in the schema
   */
  private def writeLabelDefs(edgeLabelDefs: List[EdgeLabelDef]): Unit = {
    // TODO: future: implement manyToOne(), sortKey(), etc.

    // TODO: do we want edges with certain labels to only support certain properties, seems like Titan supports this but it wasn't that way in GB2

    // TODO: implement signature, this was in GB2, not positive it is needed?
    //  ArrayList<TitanKey> titanKeys = new ArrayList<TitanKey>();
    //  signature()

    edgeLabelDefs.foreach(labelSchema => {
      val graphManager = graph.getManagementSystem()
      if (!graphManager.containsRelationType(labelSchema.label)) {
        //default multiplicity is SIMPLE (at most one edge with this label between a pair of vertices
        graphManager.makeEdgeLabel(labelSchema.label).multiplicity(Multiplicity.MULTI).make()
      }
      graphManager.commit()
    })
  }
}
