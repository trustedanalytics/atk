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
package org.trustedanalytics.atk.plugins.orientdbimport

import org.apache.spark.SparkContext
import org.apache.spark.atk.graph.VertexFrameRdd
import org.trustedanalytics.atk.plugins.orientdb.DbConfiguration
import org.apache.spark.atk.graph.GraphRddImplicits._

/**
 * imports a vertex class from OrientDB database to ATK
 *
 * @param dbConfigurations
 */
class LoadVertexFrame(dbConfigurations: DbConfiguration) {

  /**
   * A method imports a vertex class from OrientDB to ATK
   *
   * @return vertex frame RDD
   */
  def importOrientDbVertexClass(sc: SparkContext): List[VertexFrameRdd] = {

    val vertexRdd = new OrientDbVertexRdd(sc, dbConfigurations)
    val vertexFrames = vertexRdd.splitByLabel()
    vertexFrames
  }
}
