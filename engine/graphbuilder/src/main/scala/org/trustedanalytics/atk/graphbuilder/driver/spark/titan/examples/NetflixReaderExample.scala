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

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReader
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.Date

/**
 * Example of reading Titan graph in Spark.
 */
object NetflixReaderExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(ExamplesUtils.sparkMaster)
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))
    //conf.set("spark.executor.memory", "6g")
    //conf.set("spark.cores.max", "8")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")

    val sc = new SparkContext(conf)

    // Set HDFS output directory
    val resultsDir = ExamplesUtils.hdfsMaster + System.getProperty("MOVIE_RESULTS_DIR", "/user/hadoop/netflix_reader_results")
    val vertexResultsDir = resultsDir + "/vertices"
    val edgeResultsDir = resultsDir + "/edges"

    // Create graph connection
    val tableName = "netflix"
    val hBaseZookeeperQuorum = "localhost"

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", hBaseZookeeperQuorum)
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
    vertexRDD.saveAsTextFile(vertexResultsDir)
    edgeRDD.saveAsTextFile(edgeResultsDir)

    sc.stop()
  }
}
