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


package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.elements._
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema

/**
 * Write back to each vertex in Titan graph the set of communities to which it
 * belongs in some property specified by the user
 */

class CommunityWriterInTitan extends Serializable {

  /**
   * Update the graph by updating the vertices properties
   * @param gbVertices RDD of updated Graph Builder vertices list having new community property
   * @param gbEdges RDD of Graph Builder Edge list
   * @param titanConfig The titan configuration
   */
  def run(gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], titanConfig: SerializableBaseConfiguration) {

    // Create the GraphBuilder object
    // Setting true to append for updating existing graph
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))

    // Build the graph using spark
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}
