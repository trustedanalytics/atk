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


package org.trustedanalytics.atk.graphbuilder.parser

import org.trustedanalytics.atk.graphbuilder.elements._

/**
 * Parse rows of input into Edges and Vertices.
 * <p>
 * This could be used if you want to go over the input rows in a single pass.
 * </p>
 */
class CombinedParser(inputSchema: InputSchema, vertexParser: Parser[GBVertex], edgeParser: Parser[GBEdge]) extends Parser[GraphElement](inputSchema) {

  /**
   * Parse a row of data into zero to many GraphElements.
   */
  def parse(row: InputRow): Seq[GraphElement] = {
    edgeParser.parse(row) ++ vertexParser.parse(row)
  }

}
