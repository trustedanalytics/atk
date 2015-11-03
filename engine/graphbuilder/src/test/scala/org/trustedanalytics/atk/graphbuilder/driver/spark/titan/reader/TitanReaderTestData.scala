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

import java.io.File

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.core.TitanVertex
import org.apache.hadoop.io.NullWritable
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.trustedanalytics.atk.testutils.DirectoryUtils

import scala.collection.JavaConversions._

/**
 * A collection of data used to test reading from a Titan graph.
 *
 * The test data represents a subgraph of Titan's graph of the god's example in multiple formats namely as:
 * 1. Titan graph elements
 * 2. GraphBuilder graph elements
 * 3. Serialized Titan rows, where each row represents a vertex and its adjacency list
 * 4. Serialized HBase rows, where each row represents a vertex and its adjacency list
 *
 */
object TitanReaderTestData extends Suite with BeforeAndAfterAll {

  import TitanReaderUtils._

  val gbID = TitanReader.TITAN_READER_DEFAULT_GB_ID
  private var tmpDir: File = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "berkeleyje")
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  val titanConnector = new TitanGraphConnector(titanConfig)
  val graph = titanConnector.connect()

  // Create a test graph which is a subgraph of Titan's graph of the gods
  val graphManager = graph.getManagementSystem()
  graphManager.makeEdgeLabel("brother").make()
  graphManager.makeEdgeLabel("lives").make()

  // Ordering properties alphabetically to ensure to that tests pass
  // Since properties are represented as a sequence, graph elements with different property orders are not considered equal
  graphManager.makePropertyKey("age").dataType(classOf[Integer]).make()
  graphManager.makePropertyKey("name").dataType(classOf[String]).make()
  graphManager.makePropertyKey("reason").dataType(classOf[String]).make()
  graphManager.makePropertyKey("type").dataType(classOf[String]).make()
  graphManager.commit()

  // Titan graph elements
  val neptuneTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4500)
    vertex.setProperty("name", "neptune")
    vertex.setProperty("type", "god")
    vertex.asInstanceOf[TitanVertex]
  }

  val seaTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("name", "sea")
    vertex.setProperty("type", "location")
    vertex.asInstanceOf[TitanVertex]
  }

  val plutoTitanVertex = {
    val vertex = graph.addVertex(null)
    vertex.setProperty("age", 4000)
    vertex.setProperty("name", "pluto")
    vertex.setProperty("type", "god")
    vertex.asInstanceOf[TitanVertex]
  }

  val seaTitanEdge = {
    val edge = neptuneTitanVertex.addEdge("lives", seaTitanVertex)
    edge.setProperty("reason", "loves waves")
    edge
  }

  val plutoTitanEdge = neptuneTitanVertex.addEdge("brother", plutoTitanVertex)

  // GraphBuilder graph elements
  val neptuneGbVertex = {
    val gbNeptuneProperties = createGbProperties(neptuneTitanVertex.getProperties())
    new GBVertex(neptuneTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), gbNeptuneProperties)
  }

  val seaGbVertex = {
    val gbSeaProperties = createGbProperties(seaTitanVertex.getProperties())
    new GBVertex(seaTitanVertex.getId, Property(gbID, seaTitanVertex.getId), gbSeaProperties)
  }

  val plutoGbVertex = {
    val gbPlutoProperties = createGbProperties(plutoTitanVertex.getProperties())
    new GBVertex(plutoTitanVertex.getId, Property(gbID, plutoTitanVertex.getId), gbPlutoProperties)
  }

  val seaGbEdge = {
    val gbSeaEdgeProperties = Set(Property("reason", "loves waves"))
    new GBEdge(None, neptuneTitanVertex.getId, seaTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), Property(gbID, seaTitanVertex.getId), seaTitanEdge.getLabel(), gbSeaEdgeProperties)
  }

  val plutoGbEdge = {
    new GBEdge(None, neptuneTitanVertex.getId, plutoTitanVertex.getId, Property(gbID, neptuneTitanVertex.getId), Property(gbID, plutoTitanVertex.getId), plutoTitanEdge.getLabel(), Set[Property]())
  }

  // Faunus graph elements
  val neptuneFaunusVertex = createFaunusVertex(neptuneTitanVertex)
  val plutoFaunusVertex = createFaunusVertex(plutoTitanVertex)
  val seaFaunusVertex = createFaunusVertex(seaTitanVertex)

  // HBase rows
  val hBaseRows = Seq((NullWritable.get(), neptuneFaunusVertex)) //, (NullWritable.get(), plutoFaunusVertex), (NullWritable.get(), seaFaunusVertex))

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }
  }

}
