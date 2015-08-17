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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.examples

// $COVERAGE-OFF$
// This is example code only, not part of the main product

import java.util.Date

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, InputSchema }
import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Example of building a Graph using a Netflix file as input.
 */
object NetflixExampleDriver {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "hbase")
  titanConfig.setProperty("storage.table", "netflix124")
  //titanConfig.setProperty("storage.backend", "cassandra")
  //titanConfig.setProperty("storage.keyspace", "netflix")
  titanConfig.setProperty("storage.hostname", ExamplesUtils.storageHostname)
  titanConfig.setProperty("storage.batch-loading", "true")
  titanConfig.setProperty("autotype", "none")
  titanConfig.setProperty("storage.buffer-size", "2048")
  titanConfig.setProperty("storage.lock.wait.time", "400")
  titanConfig.setProperty("storage.lock.retries", "15")
  titanConfig.setProperty("storage.parallel-backend-ops", "true")
  titanConfig.setProperty("ids.authority.randomized-conflict-avoidance-retries", "30")
  titanConfig.setProperty("ids.block-size", "300000")
  titanConfig.setProperty("ids.flush", "true")
  titanConfig.setProperty("ids.renew-timeout", "120000")
  titanConfig.setProperty("ids.num-partitions", "10")
  titanConfig.setProperty("ids.authority.conflict-avoidance-mode", "GLOBAL_AUTO")

  // Input Schema
  val inputSchema = new InputSchema(List(
    new ColumnDef("userId", classOf[String]),
    new ColumnDef("vertexType", classOf[String]),
    new ColumnDef("movieId", classOf[String]),
    new ColumnDef("rating", classOf[String]),
    new ColumnDef("splits", classOf[String])))

  // Parser Configuration
  val vertexRules = List(VertexRule(property("userId"), List(property("vertexType"))), VertexRule(property("movieId")))
  val edgeRules = List(EdgeRule(property("userId"), property("movieId"), "rates", List(property("rating"), property("splits"))))

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
    // let user to pass in these parameters as command line options
    // conf.set("spark.executor.memory", "32g")
    // conf.set("spark.cores.max", "33")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")

    val sc = new SparkContext(conf)

    // Setup data in Spark
    val inputRows = sc.textFile(ExamplesUtils.movieDataset, System.getProperty("PARTITIONS", "100").toInt)
    val inputRdd = inputRows.map(row => row.split(","): Seq[_])

    // Build the Graph
    val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, append = false, broadcastVertexIds = false)
    val gb = new GraphBuilder(config)
    gb.build(inputRdd)

    println("done " + new Date())

  }

}
