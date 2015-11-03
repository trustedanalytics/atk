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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.examples

// $COVERAGE-OFF$
// This is example code only, not part of the main product

import java.util.Date

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReader
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Example of reading Titan graph in Spark.
 */
object TitanReaderExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "32")

    val sc = new SparkContext(conf)

    // Create graph connection
    val tableName = System.getProperty("TABLE_NAME", "titan")

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", "localhost")
    titanConfig.setProperty("storage.hbase.table", tableName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read graph
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    // Remember to import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._ to access filter methods
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()

    // If you encounter the following error, "com.esotericsoftware.kryo.KryoException: Buffer overflow", because
    // your results are too large, try:
    // a) Increasing the size of the kryoserializer buffer, e.g., conf.set("spark.kryoserializer.buffer.mb", "32")
    // b) Saving results to file instead of collect(), e.g.titanReaderRDD.saveToTextFile()
    //val graphElementsCount = titanReaderRDD.count()
    val vertexCount = vertexRDD.count()
    val edgeCount = edgeRDD.count()

    //println("Graph element count:" + graphElementsCount)
    println("Vertex count:" + vertexCount)
    println("Edge count:" + edgeCount)
    sc.stop()
  }
}
