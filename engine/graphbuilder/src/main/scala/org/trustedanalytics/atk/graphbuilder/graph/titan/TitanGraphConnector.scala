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

package org.trustedanalytics.atk.graphbuilder.graph.titan

import java.io.File

import org.trustedanalytics.atk.graphbuilder.graph.GraphConnector
import org.trustedanalytics.atk.graphbuilder.titan.cache.TitanGraphCache
import org.trustedanalytics.atk.graphbuilder.graph.GraphConnector
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.Graph

import org.apache.commons.configuration.{ Configuration, PropertiesConfiguration }

import scala.collection.JavaConversions._

/**
 * Get a connection to Titan.
 * <p>
 * This was needed because a 'Connector' can be serialized across the network, whereas a live connection can't.
 * </p>
 */
case class TitanGraphConnector(config: Configuration) extends GraphConnector with Serializable {

  /**
   * Initialize using a properties configuration file.
   * @param propertiesFile a .properties file, see example-titan.properties and Titan docs
   */
  def this(propertiesFile: File) {
    this(new PropertiesConfiguration(propertiesFile))
  }

  /**
   * Get a connection to a graph database.
   *
   * Returns a StandardTitanGraph which is a superset of TitanGraph. StandardTitanGraph implements additional
   * methods required to load graphs from Titan.
   */
  override def connect(): TitanGraph = {
    TitanFactory.open(config)
  }

}

object TitanGraphConnector {

  val titanGraphCache = new TitanGraphCache()

  /**
   * Helper method to resolve ambiguous reference error in TitanGraph.getVertices() in Titan 0.5.1+
   *
   * "Error:(96, 18) ambiguous reference to overloaded definition, both method getVertices in
   * trait TitanGraphTransaction of type (x$1: <repeated...>[Long])java.util.Map[Long,com.thinkaurelius.titan.core.TitanVertex]
   * and  method getVertices in trait Graph of type ()Iterable[com.tinkerpop.blueprints.Vertex]
   * match expected type ?
   */
  def getVertices(titanGraph: Graph): Iterable[com.tinkerpop.blueprints.Vertex] = {
    val vertices: Iterable[com.tinkerpop.blueprints.Vertex] = titanGraph.getVertices
    vertices
  }

  /**
   * Get Titan graph from cache
   *
   * @param titanConnector Titan connector
   * @return Titan graph
   */
  def getGraphFromCache(titanConnector: TitanGraphConnector): TitanGraph = {
    val titanGraph = titanGraphCache.getGraph(titanConnector.config)
    titanGraph
  }

  /**
   * Invalidate all entries in the cache when it is shut down
   */
  def invalidateGraphCache(): Unit = {
    titanGraphCache.invalidateAllCacheEntries
    println("Invalidating Titan graph cache:" + titanGraphCache)
  }
}
