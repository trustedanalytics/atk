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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.domain.graph.GraphEntity

//TODO: This should be replaced by a "storage" parameter on Graph and DataFrame that holds specifics like
// Titan table name and HDFS uris, etc. for those that need them.

/**
 * Utility for converting between user provided graph names and their names in the graph database.
 */
object GraphBackendName {

  private val iatGraphTablePrefix: String = "iat_graph_"

  /**
   * Converts the user's name for a graph into the name used by the underlying graph store.
   */
  def getGraphBackendName(graph: GraphEntity): String = {
    iatGraphTablePrefix + graph.id
  }

  def getIdFromBackendName(graphName: String): Long = graphName.stripPrefix(iatGraphTablePrefix).toLong

}
