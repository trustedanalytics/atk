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
// This is utility code only, not part of the main product

import java.io.File
import java.net.InetAddress

import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import com.tinkerpop.blueprints.Graph

import scala.collection.JavaConversions._

/**
 * Single location for settings used in examples to make them easier to run on different machines.
 */
object ExamplesUtils {

  val hdfsMaster = System.getProperty("HDFS_MASTER", "hdfs://" + hostname)

  /**
   * Storage hostname setting for titan.
   */
  def storageHostname: String = {
    val storageHostname = System.getProperty("STORAGE_HOSTNAME", "localhost")
    println("STORAGE_HOSTNAME: " + storageHostname)
    storageHostname
  }

  /**
   * URL to the Spark Master, from either a system property or best guess
   */
  def sparkMaster: String = {
    val sparkMaster = System.getProperty("SPARK_MASTER", "spark://" + hostname + ":7077")
    println("SPARK_MASTER: " + sparkMaster)
    sparkMaster
  }

  /**
   * Absolute path to the gb.jar file from either a system property or best guess
   */
  def gbJar: String = {
    val gbJar = System.getProperty("GB_JAR", guessGbJar)
    println("gbJar: " + gbJar)
    require(new File(gbJar).exists(), "GB_JAR does not exist")
    gbJar
  }

  /**
   * Check for the gb.jar in expected locations
   */
  private def guessGbJar: String = {
    val possiblePaths = List(
      // SBT build - should be removed after Maven build is working right
      System.getProperty("user.dir") + "/graphbuilder/target/scala-2.10/gb.jar",
      System.getProperty("user.dir") + "/target/scala-2.10/gb.jar",
      System.getProperty("user.dir") + "/gb.jar",
      // Maven build not working yet
      System.getProperty("user.dir") + "/graphbuilder/target/graphbuilder-master-SNAPSHOT.jar",
      System.getProperty("user.dir") + "/target/graphbuilder.jar",
      System.getProperty("user.dir") + "/graphbuilder.jar")
    possiblePaths.foreach(path => {
      val jar = new File(path)
      if (jar.exists()) {
        return jar.getAbsolutePath
      }
    })
    throw new RuntimeException("gb jar wasn't found at in any of the expected locations, please run 'sbt assembly' or set GB_JAR")
  }

  /**
   * Spark home directory from either a system property or best guess
   */
  def sparkHome: String = {
    val sparkHome = System.getProperty("SPARK_HOME", guessSparkHome)
    println("SPARK_HOME: " + sparkHome)
    require(new File(sparkHome).exists(), "SPARK_HOME does not exist")
    sparkHome
  }

  /**
   * Check for SPARK_HOME in the expected locations
   */
  private def guessSparkHome: String = {
    val possibleSparkHomes = List("/opt/cloudera/parcels/CDH/lib/spark/", "/usr/lib/spark")
    possibleSparkHomes.foreach(dir => {
      val path = new File(dir)
      if (path.exists()) {
        return path.getAbsolutePath
      }
    })
    throw new RuntimeException("SPARK_HOME wasn't found at any of the expected locations, please set SPARK_HOME")
  }

  /** Hostname for current system */
  private def hostname: String = InetAddress.getLocalHost.getHostName

  /**
   * Path to the movie data set.
   */
  def movieDataset: String = {
    val moviePath = System.getProperty("MOVIE_DATA", "/user/hadoop/netflix.csv")
    println("Movie Data Set in HDFS: " + moviePath)
    hdfsMaster + moviePath
  }

  /**
   * Dump the entire graph into a String (not scalable obviously but nice for quick testing)
   */
  def dumpGraph(graph: Graph): String = {
    var vertexCount = 0
    var edgeCount = 0

    val output = new StringBuilder("---- Graph Dump ----\n")

    TitanGraphConnector.getVertices(graph).toList.foreach(v => {
      output.append(v).append("\n")
      vertexCount += 1
    })

    graph.getEdges.toList.foreach(e => {
      output.append(e).append("\n")
      edgeCount += 1
    })

    output.append(vertexCount + " Vertices, " + edgeCount + " Edges")

    output.toString()
  }
}
