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

package org.trustedanalytics.atk.engine

import org.trustedanalytics.atk.engine.plugin.Invocation

/**
 * This manages the backend storage for graphs, underneath the graph database.
 *
 */
trait GraphBackendStorage {
  def deleteUnderlyingTable(graphName: String, quiet: Boolean, inBackground: Boolean = false)(implicit invocation: Invocation)
  def copyUnderlyingTable(graphName: String, name: String)(implicit invocation: Invocation)
}
