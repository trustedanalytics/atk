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


package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GraphElement, Property, GBVertex }
import com.thinkaurelius.titan.core.{ TitanProperty, TitanVertex }
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.tinkerpop.blueprints.Direction

import scala.collection.JavaConversions._

/**
 * Utility methods for creating test data for reading Titan graphs.
 *
 * These utilities serialize Titan graph elements into the format that Titan uses for its key-value stores.
 * The utilities also create GraphBuilder elements from Titan elements.
 */
object TitanReaderUtils {

  /**
   * Create GraphBuilder properties from a list of Titan properties.
   *
   * @param properties Titan properties
   * @return Iterable of GraphBuilder properties
   */
  def createGbProperties(properties: Iterable[TitanProperty]): Set[Property] = {
    properties.map(p => Property(p.getPropertyKey().getName(), p.getValue())).toSet
  }

  /**
   * Orders properties in GraphBuilder elements alphabetically using the property key.
   *
   * Needed to ensure to that comparison tests pass. Graphbuilder properties are represented
   * as a sequence, so graph elements with different property orderings are not considered equal.
   *
   * @param graphElements Array of GraphBuilder elements
   * @return  Array of GraphBuilder elements with sorted property lists
   */
  def sortGraphElementProperties(graphElements: Array[GraphElement]) = {
    graphElements.map {
      case v: GBVertex => {
        new GBVertex(v.physicalId, v.gbId, v.properties).asInstanceOf[GraphElement]
      }
      case e: GBEdge => {
        new GBEdge(None, e.tailPhysicalId, e.headPhysicalId, e.tailVertexGbId, e.headVertexGbId, e.label, e.properties).asInstanceOf[GraphElement]
      }
    }
  }

  def createFaunusVertex(titanVertex: TitanVertex): FaunusVertex = {
    val faunusVertex = new FaunusVertex()
    faunusVertex.setId(titanVertex.getLongId)

    titanVertex.getProperties().map(property => {
      faunusVertex.addProperty(property.getPropertyKey().getName(), property.getValue())
    })

    titanVertex.getTitanEdges(Direction.OUT).map(edge => {
      val faunusEdge = faunusVertex.addEdge(edge.getLabel(), edge.getOtherVertex(titanVertex))
      edge.getPropertyKeys().map(property => faunusEdge.setProperty(property, edge.getProperty(property)))
    })
    faunusVertex
  }
}
