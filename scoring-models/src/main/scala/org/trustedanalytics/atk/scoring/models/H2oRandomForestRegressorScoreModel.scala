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

package org.trustedanalytics.atk.scoring.models

import hex.genmodel.GenModel
import org.apache.spark.h2o.H2oScoringModelData
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }
import scala.collection.JavaConverters._

/**
 * Scoring model for H2O's RandomForest
 * @param genModel H2O scoring model
 */
class H2oRandomForestRegressorScoreModel(h2oModelData: H2oScoringModelData, genModel: GenModel) extends Model {

  override def score(data: Array[Any]): Array[Any] = {
    val x: Array[Double] = data.map(d => ScoringModelUtils.asDouble(d))
    val fields = h2oModelData.observationColumns.zip(x).map {
      case (name, value) => (name, double2Double(value))
    }.toMap
    val score = genModel.score0(fields.asJava)

    data :+ score(0)
  }

  /**
   *  @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    val obsCols = h2oModelData.observationColumns
    var input = Array[Field]()
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("H2O Random Forest Regressor Model", classOf[H2oRandomForestRegressorScoreModel].getName, classOf[H2oRandomForestRegressorModelReaderPlugin].getName, Map())
  }

  /**
   *  @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("Prediction", "Double")
  }
}
