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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.trustedanalytics.atk.scoring.models.ARXData
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import com.cloudera.sparkts.models.ARXModel

/** Json conversion for arguments and return value case classes */
object ARXJsonProtocol {

  implicit object ARXModelFormat extends JsonFormat[ARXModel] {
    /**
     * The write methods converts from ARXModel to JsValue
     * @param obj ARXModel. Where ARXModel's format is
     *            c : scala.Double
     *            coefficients : scala.Array[scala.Double]
     *            yMaxLag : scala.Int
     *            xMaxLag : scala.Int
     * @return JsValue
     */
    override def write(obj: ARXModel): JsValue = {
      JsObject(
        "c" -> obj.c.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "xMaxLag" -> obj.xMaxLag.toJson,
        "yMaxLag" -> obj.yMaxLag.toJson
      // NOTE: unable to save includesOriginalX parameter
      )
    }

    /**
     * The read method reads a JsValue to ARXModel
     * @param json JsValue
     * @return ARXModel with format
     *         c : scala.Double
     *         coefficients : scala.Array[scala.Double]
     *         yMaxLag : scala.Int
     *         xMaxLag : scala.Int
     */
    override def read(json: JsValue): ARXModel = {
      val fields = json.asJsObject.fields
      val c = getOrInvalid(fields, "c").convertTo[Double]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val xMaxLag = getOrInvalid(fields, "xMaxLag").convertTo[Int]
      val yMaxLag = getOrInvalid(fields, "yMaxLag").convertTo[Int]
      // NOTE: unable to get includesOriginalX - defaulting to true
      new ARXModel(c, coefficients, xMaxLag, yMaxLag, true)
    }
  }

  implicit val arxPredictArgsFormat = jsonFormat4(ARXPredictArgs)
  implicit val arxTrainArgsFormat = jsonFormat7(ARXTrainArgs)
  implicit val arxTrainReturnFormat = jsonFormat2(ARXTrainReturn)
  implicit val arxDataFormat = jsonFormat2(ARXData)

}
