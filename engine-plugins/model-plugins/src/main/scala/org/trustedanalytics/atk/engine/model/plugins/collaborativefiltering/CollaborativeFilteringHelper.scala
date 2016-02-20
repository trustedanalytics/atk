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
package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ DataTypes }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Helper class for collaborative filtering*/
object CollaborativeFilteringHelper {
  def toAlsRdd(userFrame: FrameRdd, data: CollaborativeFilteringData): RDD[(Int, Array[Double])] = {
    val idColName = data.userFrame.schema.column(0).name
    val featuresColName = data.userFrame.schema.column(1).name

    userFrame.mapRows(row => (row.intValue(idColName),
      DataTypes.vector.parse(row.value(featuresColName)).get.toArray))
  }

}
