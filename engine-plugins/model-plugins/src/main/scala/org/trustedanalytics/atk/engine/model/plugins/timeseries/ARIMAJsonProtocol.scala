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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import spray.json._
import com.cloudera.sparkts.models.ARIMAModel
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.scoring.models.ARIMAData

object ARIMAJsonProtocol {

  implicit object ARIMAModelFormat extends JsonFormat[ARIMAModel] {

    /**
     * Converts from an ARIMAModel to a JsValue
     * @param obj ARIMAModel where the format is
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            hasIntercept : scala.Boolean
     * @return JsValue
     */
    override def write(obj: ARIMAModel): JsValue = {
      JsObject(
        "p" -> obj.p.toJson,
        "d" -> obj.d.toJson,
        "q" -> obj.q.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "hasIntercept" -> obj.hasIntercept.toJson
      )
    }

    /**
     * Reads a JsValue and returns an ARIMAModel.
     * @param json JsValue
     * @return ARIMAModel where the format is
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            hasIntercept : scala.Boolean
     */
    override def read(json: JsValue): ARIMAModel = {
      val fields = json.asJsObject.fields
      val p = getOrInvalid(fields, "p").convertTo[Int]
      val d = getOrInvalid(fields, "d").convertTo[Int]
      val q = getOrInvalid(fields, "q").convertTo[Int]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val hasIntercept = getOrInvalid(fields, "hasIntercept").convertTo[Boolean]
      new ARIMAModel(p, d, q, coefficients, hasIntercept)
    }
  }

  implicit val arimaPredictArgsFormat = jsonFormat4(ARIMAPredictArgs)
  implicit val arimaTrainArgsFormat = jsonFormat9(ARIMATrainArgs)
  implicit val arimaTrainReturnFormat = jsonFormat1(ARIMATrainReturn)
  implicit val arimaDataFormat = jsonFormat1(ARIMAData)
}
