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
package org.trustedanalytics.atk.plugins.orientdb

import org.trustedanalytics.atk.engine.plugin.ArgDoc
import scala.collection.immutable.Map

/**
 * returns the output arguments of ExportOrientDbGraphPlugin
 * @param exportedVertices a dictionary of vertex classname and the corresponding statistics of exported vertices
 * @param exportedEdges a dictionary of edge classname and the corresponding statistics of exported edges.
 * @param dbUri the database URI
 */

case class ExportOrientDbGraphReturn(@ArgDoc(
                                       """a dictionary of vertex classname,
    |and the corresponding statistics of exported vertices.""") exportedVertices: Map[String, Statistics],
                                     @ArgDoc(
                                       """a dictionary of edge classname,
                                         |and the corresponding statistics of exported edges.""") exportedEdges: Map[String, Statistics],
                                     @ArgDoc("""The URI to the OrientDB graph .""") dbUri: String)

/**
 * returns statistics for the exported graph elements
 * @param exportedCount the number of the exported elements
 * @param failuresCount the number of elements failed to be exported.
 */
case class Statistics(exportedCount: Long, failuresCount: Long)