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
package org.trustedanalytics.atk.domain.graph.construction

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.GraphSchema

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for adding Vertices to a Vertex Frame
 */
case class AddVerticesArgs(vertexFrame: FrameReference,
                           @ArgDoc("""Frame that will be the source of
the vertex data.""") sourceFrame: FrameReference,
                           @ArgDoc("""Column name for a unique id for each vertex.""") idColumnName: String,
                           @ArgDoc("""Column names that will be turned
into properties for each vertex.""") columnNames: Option[Seq[String]] = None) {
  allColumnNames.foreach(name => require(!GraphSchema.isVertexSystemColumn(name), s"$name can't be used as an input column name, it is reserved for system use"))

  /**
   * All of the column names (idColumn plus the rest)
   */
  def allColumnNames: List[String] = {
    if (columnNames.isDefined) {
      List(idColumnName) ++ columnNames.get.toList
    }
    else {
      List(idColumnName)
    }
  }

}
