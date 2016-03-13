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
package org.trustedanalytics.atk.graphbuilder.rdd

import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }

/**
 * Functions that are applicable to Vertex RDD's.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */
class VertexRddFunctions(self: RDD[GBVertex]) {

  /**
   * Merge duplicate Vertices, creating a new Vertex that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[GBVertex] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * Convert "Vertices with or without _label property" into "Vertices with _label property"
   * @param indexNames Vertex properties that have been indexed (fallback for labels)
   * @return Vertices with _label property
   */
  def labelVertices(indexNames: List[String]): RDD[GBVertex] = {
    self.map(vertex => {
      val columnNames = vertex.fullProperties.map(_.key)
      val indexedProperties = indexNames.intersect(columnNames.toSeq)
      val userDefinedColumn = indexedProperties.headOption

      val label = if (vertex.getProperty(GraphSchema.labelProperty).isDefined && vertex.getProperty(GraphSchema.labelProperty).get.value != null) {
        vertex.getProperty(GraphSchema.labelProperty).get.value
      }
      else if (userDefinedColumn.isDefined) {
        userDefinedColumn.get
      }
      else {
        "unlabeled"
      }
      val props = vertex.properties.filter(_.key != GraphSchema.labelProperty) + Property(GraphSchema.labelProperty, label)
      new GBVertex(vertex.physicalId, vertex.gbId, props)
    })
  }

}
