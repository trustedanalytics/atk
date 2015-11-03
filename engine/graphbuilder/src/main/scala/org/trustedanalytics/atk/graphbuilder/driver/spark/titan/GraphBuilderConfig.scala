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


package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import org.trustedanalytics.atk.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ EdgeRule, VertexRule }

/**
 * Configuration options for GraphBuilder
 *
 * @param inputSchema describes the columns of input
 * @param vertexRules rules for parsing Vertices
 * @param edgeRules rules for parsing Edges
 * @param titanConfig connect to Titan
 * @param append true to append to an existing Graph, slower because each write requires a read (incremental graph construction).
 * @param retainDanglingEdges true to add extra vertices for dangling edges, false to drop dangling edges
 * @param inferSchema true to automatically infer the schema from the rules and, if needed, the data, false if the schema is already defined.
 * @param broadcastVertexIds experimental feature where gbId to physicalId mappings will be broadcast by spark instead
 *                           of doing the usual an RDD JOIN.  Should only be true if all of the vertices can fit in
 *                           memory of both the driver and each executor.
 *                           Theoretically, this should be faster but shouldn't scale as large as when it is false.
 *                           This feature does not perform well yet for larger data sizes (it is hitting some other
 *                           bottleneck while writing edges). 23GB Netflix data produced about 180MB of Vertex Ids.
 */
case class GraphBuilderConfig(inputSchema: InputSchema,
                              vertexRules: List[VertexRule],
                              edgeRules: List[EdgeRule],
                              titanConfig: SerializableBaseConfiguration,
                              append: Boolean = false,
                              retainDanglingEdges: Boolean = false,
                              inferSchema: Boolean = true,
                              broadcastVertexIds: Boolean = false) extends Serializable {

}
