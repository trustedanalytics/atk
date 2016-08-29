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

import com.cloudera.sparkts.models.ARIMAXModel
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.scoring.models.ARIMAXData
import spray.json._

/** Json conversion for arguments and return value case classes */
object ARIMAXJsonProtocol {

  implicit object ARIMAXModelFormat extends JsonFormat[ARIMAXModel] {
    /**
     * The write methods converts from ARIMAXModel to JsValue
     * @param obj ARIMAXModel. Where ARIMAXModel's format is
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            xregMaxLag : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            includeOriginalXreg : scala.Boolean
     *            hasIntercept : scala.Boolean
     * @return JsValue
     */
    override def write(obj: ARIMAXModel): JsValue = {
      JsObject(
        "p" -> obj.p.toJson,
        "d" -> obj.d.toJson,
        "q" -> obj.q.toJson,
        "xregMaxLag" -> obj.q.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "includeOriginalXreg" -> obj.includesOriginalXreg.toJson,
        "hasIntercept" -> obj.includesOriginalXreg.toJson
      )
    }

    /**
     * The read method reads a JsValue to ARIMAXModel
     * @param json JsValue
     * @return ARIMAXModel with format
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            xregMaxLag : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            includeOriginalXreg : scala.Boolean
     *            hasIntercept : scala.Boolean
     */
    override def read(json: JsValue): ARIMAXModel = {
      val fields = json.asJsObject.fields
      val p = getOrInvalid(fields, "p").convertTo[Int]
      val d = getOrInvalid(fields, "d").convertTo[Int]
      val q = getOrInvalid(fields, "q").convertTo[Int]
      val xregMaxLag = getOrInvalid(fields, "xregMaxLag").convertTo[Int]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val includeOriginalXreg = getOrInvalid(fields, "includeOriginalXreg").convertTo[Boolean]
      val hasIntercept = getOrInvalid(fields, "hasIntercept").convertTo[Boolean]

      new ARIMAXModel(p, d, q, xregMaxLag, coefficients, includeOriginalXreg, hasIntercept)
    }
  }

  implicit val arimaxTrainArgsFormat = jsonFormat11(ARIMAXTrainArgs)
  implicit val arimaxTrainReturnFormat = jsonFormat4(ARIMAXTrainReturn)
  implicit val arimaxDataFormat = jsonFormat2(ARIMAXData)
  implicit val arimaxPredictArgsFormat = jsonFormat4(ARIMAXPredictArgs)
}
