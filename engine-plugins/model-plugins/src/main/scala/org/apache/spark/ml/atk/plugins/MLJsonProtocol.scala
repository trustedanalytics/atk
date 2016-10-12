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

package org.apache.spark.ml.atk.plugins

import org.apache.spark.ml.regression.CoxPhModel
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol.VectorFormat
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.regression._
import org.trustedanalytics.atk.engine.model.plugins.survivalanalysis.{ CoxPhPredictArgs, CoxPhData, CoxPhTrainReturn, CoxPhTrainArgs }
import org.trustedanalytics.atk.scoring.models.LinearRegressionData
import spray.json._

/**
 * Implicit conversions for Logistic Regression objects to/from JSON
 */

object MLJsonProtocol {

  implicit object LinearRegressionMlModelFormat extends JsonFormat[org.apache.spark.ml.regression.LinearRegressionModel] {
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

  implicit object CoxModelFormat extends JsonFormat[CoxPhModel] {

    override def write(obj: CoxPhModel): JsValue = {
      val beta = VectorFormat.write(obj.beta)
      val mean = VectorFormat.write(obj.meanVector)
      JsObject(
        "uid" -> JsString(obj.uid),
        "beta" -> beta,
        "mean" -> mean
      )
    }

    override def read(json: JsValue): org.apache.spark.ml.regression.CoxPhModel = {
      val fields = json.asJsObject.fields
      val uid = getOrInvalid(fields, "uid").convertTo[String]
      val beta = fields.get("beta").map(v => {
        VectorFormat.read(v)
      }).get

      val mean = fields.get("mean").map(v => {
        VectorFormat.read(v)
      }).get

      new CoxPhModel(uid, beta, mean)
    }
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

  implicit val linearRegressionMlModelTrainArgs = jsonFormat10(LinearRegressionTrainArgs)
  implicit val linearRegressionMlModelReturnArgs = jsonFormat11(LinearRegressionTrainReturn)
  implicit val linearRegressionMlDataFormat = jsonFormat3(LinearRegressionData)
  implicit val linearRegressionMlModelPredictArgs = jsonFormat3(LinearRegressionPredictArgs)
  implicit val linearRegressionMlModelTestArgs = jsonFormat4(LinearRegressionTestArgs)
  implicit val linearRegressionMlModelTestReturn = jsonFormat5(LinearRegressionTestReturn)
  implicit val lassoModelTestReturnFormat = jsonFormat4(LassoTestReturn)

  implicit val coxMlModelTrainArgs = jsonFormat7(CoxPhTrainArgs)
  implicit val coxMlModelTrainReturn = jsonFormat2(CoxPhTrainReturn)
  implicit val coxMlModelDataFormat = jsonFormat4(CoxPhData)
  implicit val coxMlModelPredictArgs = jsonFormat4(CoxPhPredictArgs)

}
class InvalidJsonException(message: String) extends RuntimeException(message)
