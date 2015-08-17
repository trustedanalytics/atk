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

package org.trustedanalytics.atk.domain.graph.construction

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.GraphSchema

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for adding Edges to a Edge Frame
 */
case class AddEdgesArgs(@ArgDoc("""The frame being operated on.""") edgeFrame: FrameReference,
                        @ArgDoc("""Frame that will be the source of
the edge data.""") sourceFrame: FrameReference,
                        @ArgDoc("""column name for a unique id for
each source vertex (this is not the system defined _vid).""") columnNameForSourceVertexId: String,
                        @ArgDoc("""column name for a unique id for
each destination vertex (this is not the system defined _vid).""") columnNameForDestVertexId: String,
                        @ArgDoc("""Column names to be used as properties for each vertex,
None means use all columns,
empty list means use none.""") columnNames: Option[Seq[String]] = None,
                        @ArgDoc("""True to create missing vertices for edge (slightly slower),
False to drop edges pointing to missing vertices.
Defaults to False.""") createMissingVertices: Option[Boolean] = Some(false)) {
  require(edgeFrame != null, "edge frame is required")
  require(sourceFrame != null, "source frame is required")
  require(columnNameForSourceVertexId != null, "column name for source vertex id is required to create edges")
  require(columnNameForDestVertexId != null, "column name for destination vertex id is required to create edges")
  allColumnNames.foreach(name => require(!GraphSchema.isEdgeSystemColumn(name), s"$name can't be used as an input column name, it is reserved for system use"))

  /**
   * All of the column names (idColumn plus the rest)
   */
  def allColumnNames: List[String] = {
    List(columnNameForSourceVertexId, columnNameForDestVertexId) ++ columnNames.getOrElse(Nil).toList
  }

  /**
   * true to create extra vertices if needed (converts None to false)
   */
  def isCreateMissingVertices: Boolean = createMissingVertices.getOrElse(false)

}
