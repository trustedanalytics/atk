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
package org.trustedanalytics.atk.plugins

import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }

object GBVertexUtils {

  type GBVertexPropertyPair = (GBVertex, Property)

  def mergeResults(resultRDD: RDD[(Long, Property)], gbVertexRDD: RDD[GBVertex]): RDD[GBVertex] = {
    gbVertexRDD
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(resultRDD)
      .map(vertex => generateGBVertex(vertex))
  }

  private def generateGBVertex(joinValuePair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, labelProperty) = joinValuePair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + labelProperty)
  }
}
