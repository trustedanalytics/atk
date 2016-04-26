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

import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * ExportOrientDbGraph plugin input arguments
 */

case class ExportOrientDbGraphArgs(graph: GraphReference, @ArgDoc("""OrientDB database name.""") dbName: String,
                                   @ArgDoc("""batch size for commiting transactions.""") batchSize: Int = 1000) {

  require(graph != null, "graph is required")
  require(dbName != null, "database name is required")
  require(batchSize > 0, "batch size should be a positive value")

}
