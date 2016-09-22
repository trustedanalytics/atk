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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.frame.FrameRdd
import org.apache.spark.h2o.H2oModelData
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column }
import scala.collection.JavaConverters._
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import scala.collection.mutable.ArrayBuffer

object H2oRandomForestRegressorFunctions extends Serializable {

  /**
   * Predict value for the observation columns using trained random forest model
   *
   * @param trainFrame Training frame
   * @param h2oModelData Trained model
   * @param obsColumns Observation columns
   * @return Frame with predicted value
   */
  def predict(trainFrame: FrameRdd, h2oModelData: H2oModelData, obsColumns: List[String]): FrameRdd = {
    val predictColumn = Column("predicted_value", DataTypes.float64)
    val predictRows = trainFrame.mapPartitionRows(rows => {
      val scores = new ArrayBuffer[Row]()
      val preds: Array[Double] = new Array[Double](1)
      val genModel = h2oModelData.toGenModel

      while (rows.hasNext) {
        val row = rows.next()
        val point = row.valuesAsDenseVector(obsColumns).toArray
        val fields = obsColumns.zip(point).map { case (name, value) => (name, double2Double(value)) }.toMap
        val score = genModel.score0(fields.asJava, preds)
        scores += row.addValue(score(0))
      }
      scores.toIterator
    })

    new FrameRdd(trainFrame.frameSchema.addColumn(predictColumn), predictRows)
  }
}
