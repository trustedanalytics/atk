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

import org.trustedanalytics.atk.graphbuilder.elements.{ GraphElement, GBEdge }
import org.trustedanalytics.atk.graphbuilder.elements._
import org.apache.spark.rdd.RDD

/**
 * These implicits can be imported to add GraphBuilder related functions to RDD's
 */
object GraphBuilderRddImplicits {

  /**
   * Functions applicable to RDD's that can be input to GraphBuilder Parsers
   */
  implicit def inputRDDToParserRDDFunctions(rdd: RDD[Seq[_]]) = new ParserRddFunctions(rdd)

  /**
   * Functions applicable to Vertex RDD's
   */
  implicit def vertexRDDToVertexRDDFunctions(rdd: RDD[GBVertex]) = new VertexRddFunctions(rdd)

  /**
   * Functions applicable to Edge RDD's
   */
  implicit def edgeRDDToEdgeRDDFunctions(rdd: RDD[GBEdge]) = new EdgeRddFunctions(rdd)

  /**
   * Functions applicable to GraphElement RDD's
   */
  implicit def graphElementRDDToGraphElementRDDFunctions(rdd: RDD[GraphElement]) = new GraphElementRddFunctions(rdd)

}
