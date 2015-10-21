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

import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema }

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for defining an edge
 */
case class DefineEdgeArgs(graphRef: GraphReference,
                          @ArgDoc("""Label of the edge type.""") label: String,
                          @ArgDoc("""The source "type" of vertices this edge
connects.""") srcVertexLabel: String,
                          @ArgDoc("""The destination "type" of vertices this
edge connects.""") destVertexLabel: String,
                          @ArgDoc("""True if edges are directed,
false if they are undirected.""") directed: Boolean = false) {

  def edgeSchema: EdgeSchema = {
    new EdgeSchema(GraphSchema.edgeSystemColumns, label, srcVertexLabel, destVertexLabel, directed)
  }
}
