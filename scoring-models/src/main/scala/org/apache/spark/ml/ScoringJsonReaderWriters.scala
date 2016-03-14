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

package org.apache.spark.ml

import org.apache.spark.mllib.ScoringJsonReaderWriters._
import org.trustedanalytics.atk.scoring.models._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Map

/**
 * Implicit conversions for Logistic Regression objects to/from JSON
 */

object ScoringJsonReaderWriters {

  implicit object LinearRegressionModelFormat extends JsonFormat[org.apache.spark.ml.regression.LinearRegressionModel] {
    override def write(obj: org.apache.spark.ml.regression.LinearRegressionModel): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "uid" -> JsString(obj.uid),
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    override def read(json: JsValue): org.apache.spark.ml.regression.LinearRegressionModel = {
      val fields = json.asJsObject.fields
      val uid = getOrInvalid(fields, "uid").convertTo[String]
      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }).get
      val intercept = getOrInvalid(fields, "intercept").asInstanceOf[JsNumber].value.doubleValue()
      new org.apache.spark.ml.regression.LinearRegressionModel(uid, weights, intercept)
    }
  }

  implicit object LinearRegressionDataFormat extends JsonFormat[LinearRegressionData] {
    /**
     * The write methods converts from LinearRegressionData to JsValue
     * @param obj LinearRegressionData. Where LinearRegressionData format is:
     *            LinearRegressionData(linRegModel: LinearRegressionModel, observationColumns: List[String])
     * @return JsValue
     */
    override def write(obj: LinearRegressionData): JsValue = {
      val model = LinearRegressionModelFormat.write(obj.model)
      JsObject("model" -> model,
        "observation_columns" -> obj.observationColumns.toJson,
        "label_column" -> obj.labelColumn.toJson)
    }

    /**
     * The read method reads a JsValue to LinearRegressionData
     * @param json JsValue
     * @return LinearRegressionData with format LinearRegressionData(linRegModel: LinearRegressionModel, observationColumns: List[String], label:String)
     */
    override def read(json: JsValue): LinearRegressionData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val labelColumn = getOrInvalid(fields, "label_column").convertTo[String]
      val model = fields.get("model").map(v => {
        LinearRegressionModelFormat.read(v)
      }
      ).get
      new LinearRegressionData(model, obsCols, labelColumn)
    }
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

}

class InvalidJsonException(message: String) extends RuntimeException(message)
