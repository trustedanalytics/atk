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

import com.cloudera.sparkts.models.{ ARIMAXModel => MAXModel }
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.scoring.models.MAXData
import spray.json._

/** Json conversion for arguments and return value case classes */
object MAXJsonProtocol {

  implicit object MAXModelFormat extends JsonFormat[MAXModel] {
    /**
     * The write methods converts from MAXModel to JsValue
     * @param obj MAXModel. Where MAXModel's format is
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            xregMaxLag : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            includeOriginalXreg : scala.Boolean
     *            includeIntercept : scala.Boolean
     * @return JsValue
     */
    override def write(obj: MAXModel): JsValue = {
      JsObject(
        "p" -> obj.p.toJson,
        "d" -> obj.d.toJson,
        "q" -> obj.q.toJson,
        "xregMaxLag" -> obj.xregMaxLag.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "includeOriginalXreg" -> obj.includeOriginalXreg.toJson,
        "includeIntercept" -> obj.includeIntercept.toJson
      )
    }

    /**
     * The read method reads a JsValue to MAXModel
     * @param json JsValue
     * @return MAXModel with format
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            xregMaxLag : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            includeOriginalXreg : scala.Boolean
     *            includeIntercept : scala.Boolean
     */
    override def read(json: JsValue): MAXModel = {
      val fields = json.asJsObject.fields
      val p = getOrInvalid(fields, "p").convertTo[Int]
      val d = getOrInvalid(fields, "d").convertTo[Int]
      val q = getOrInvalid(fields, "q").convertTo[Int]
      val xregMaxLag = getOrInvalid(fields, "xregMaxLag").convertTo[Int]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val includeOriginalXreg = getOrInvalid(fields, "includeOriginalXreg").convertTo[Boolean]
      val includeIntercept = getOrInvalid(fields, "includeIntercept").convertTo[Boolean]

      new MAXModel(p, d, q, xregMaxLag, coefficients, includeOriginalXreg, includeIntercept)
    }
  }

  implicit val maxTrainArgsFormat = jsonFormat9(MAXTrainArgs)
  implicit val maxTrainReturnFormat = jsonFormat4(MAXTrainReturn)
  implicit val maxDataFormat = jsonFormat2(MAXData)
  implicit val maxPredictArgsFormat = jsonFormat4(MAXPredictArgs)
}
