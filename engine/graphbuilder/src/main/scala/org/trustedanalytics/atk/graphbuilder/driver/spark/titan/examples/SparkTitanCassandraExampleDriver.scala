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
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, InputSchema }
import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * This is an example of building a graph on Titan with a Cassandra backend using Spark.
 *
 * This example uses RuleParsers
 */
object SparkTitanCassandraExampleDriver {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "titan" + System.currentTimeMillis())

  // Input Data
  val inputRows = List(
    List("1", "{(1)}", "1", "Y", "1", "Y"),
    List("2", "{(1)}", "10", "Y", "2", "Y"),
    List("3", "{(1)}", "11", "Y", "3", "Y"),
    List("4", "{(1),(2)}", "100", "N", "4", "Y"),
    List("5", "{(1)}", "101", "Y", "5", "Y"))

  // Input Schema
  val inputSchema = new InputSchema(List(
    new ColumnDef("cf:number", classOf[String]),
    new ColumnDef("cf:factor", classOf[String]),
    new ColumnDef("binary", classOf[String]),
    new ColumnDef("isPrime", classOf[String]),
    new ColumnDef("reverse", classOf[String]),
    new ColumnDef("isPalindrome", classOf[String])))

  // Parser Configuration
  val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))),
    VertexRule(gbId("reverse")))
  val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

  /**
   * This is an example of building a graph on Titan with a Cassandra backend using Spark.
   */
  def main(args: Array[String]) {

    println("start " + new Date())

    // Initialize Spark Connection
    val conf = new SparkConf()
      .setMaster(ExamplesUtils.sparkMaster)
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))
    val sc = new SparkContext(conf)

    // Setup data in Spark
    val inputRdd = sc.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

    // Build the Graph
    val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, append = false)
    val gb = new GraphBuilder(config)
    gb.build(inputRdd)

    // Print the Graph
    val titanConnector = new TitanGraphConnector(titanConfig)
    val graph = titanConnector.connect()
    try {
      println(ExamplesUtils.dumpGraph(graph))
    }
    finally {
      graph.shutdown()
    }

    println("done " + new Date())

  }

}
