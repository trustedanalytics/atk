/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.domain.schema

/**
 * Utility methods and constants used in Schemas for Seamless Graphs
 *
 * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
 * The same data can be treated as frames one moment and as a graph the next without any import/export.
 */
object GraphSchema {

  val labelProperty = "_label"
  val vidProperty = "_vid"
  val edgeProperty = "_eid"
  val srcVidProperty = "_src_vid"
  val destVidProperty = "_dest_vid"

  val vertexSystemColumns = Column(vidProperty, DataTypes.int64) :: Column(labelProperty, DataTypes.string) :: Nil
  /** ordered list */
  val vertexSystemColumnNames = vertexSystemColumns.map(column => column.name)
  val vertexSystemColumnNamesSet = vertexSystemColumnNames.toSet
  val edgeSystemColumns = Column(GraphSchema.edgeProperty, DataTypes.int64) ::
    Column(srcVidProperty, DataTypes.int64) ::
    Column(destVidProperty, DataTypes.int64) ::
    Column(labelProperty, DataTypes.string) ::
    Nil

  /** ordered list */
  val edgeSystemColumnNames = edgeSystemColumns.map(column => column.name)
  val edgeSystemColumnNamesSet = edgeSystemColumnNames.toSet

  def isVertexSystemColumn(name: String): Boolean = {
    vertexSystemColumnNames.contains(name)
  }

  def isEdgeSystemColumn(name: String): Boolean = {
    edgeSystemColumnNames.contains(name)
  }
}
