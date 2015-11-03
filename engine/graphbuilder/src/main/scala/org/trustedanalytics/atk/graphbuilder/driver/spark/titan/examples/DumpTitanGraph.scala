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

import java.util.Date

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector

/**
 * Utility for use during development.
 */
object DumpTitanGraph {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "netflix")

  def main(args: Array[String]) = {

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
