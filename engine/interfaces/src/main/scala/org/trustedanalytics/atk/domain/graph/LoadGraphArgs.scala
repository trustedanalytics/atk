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

package org.trustedanalytics.atk.domain.graph

import org.trustedanalytics.atk.domain.graph.construction.FrameRule

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading  graph data into existing graph in the graph database. Source is tabular data from a dataframe
 * and it is converted into graph data using the graphbuilder3 graph construction rules.
 * @param graph Handle to the graph to be written to.
 * @param frameRules List of handles to the dataframe to be used as a data source.
 * @param append true to append to an existing graph, false otherwise.
 */
case class LoadGraphArgs(graph: GraphReference,
                         frameRules: List[FrameRule],
                         append: Boolean = false) {
  require(graph != null, "graph must not be null")
  require(frameRules != null, "frame rules must not be null")
}
