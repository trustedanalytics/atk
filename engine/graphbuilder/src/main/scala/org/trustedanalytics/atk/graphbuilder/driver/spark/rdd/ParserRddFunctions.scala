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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.elements.{ GraphElement, GBEdge, GBVertex }
import org.trustedanalytics.atk.graphbuilder.parser.Parser
import org.trustedanalytics.atk.graphbuilder.elements._
import org.trustedanalytics.atk.graphbuilder.parser.Parser
import org.apache.spark.rdd.RDD

/**
 * Functions for RDD's that can act as input to GraphBuilder.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */

class ParserRddFunctions(self: RDD[Seq[_]]) {

  /**
   * Parse the raw rows of input into Vertices
   *
   * @param vertexParser the parser to use
   */
  def parseVertices(vertexParser: Parser[GBVertex]): RDD[GBVertex] = {
    new VertexParserRdd(self, vertexParser)
  }

  /**
   * Parse the raw rows of input into Edges
   *
   * @param edgeParser the parser to use
   */
  def parseEdges(edgeParser: Parser[GBEdge]): RDD[GBEdge] = {
    self.flatMap(row => edgeParser.parse(row))
  }

  /**
   * Parse the raw rows of input into GraphElements.
   * <p>
   * This method is useful if you want to make a single pass over the input
   * to parse both Edges and Vertices..
   * </p>
   * @param parser the parser to use
   */
  def parse(parser: Parser[GraphElement]): RDD[GraphElement] = {
    self.flatMap(row => parser.parse(row))
  }

}
