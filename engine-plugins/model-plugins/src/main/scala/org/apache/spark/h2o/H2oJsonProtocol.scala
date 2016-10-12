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

package org.apache.spark.h2o

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.mllib.atk.plugins.InvalidJsonException
import spray.json._

/**
 * Implicit conversions for H2O model objects to/from JSON
 */
object H2oJsonProtocol {

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

  implicit object H2oModelFormat extends JsonFormat[H2oModelData] {
    override def read(json: JsValue): H2oModelData = {
      val fields = json.asJsObject.fields
      val modelName = getOrInvalid(fields, "model_name").convertTo[String]
      val pojo = getOrInvalid(fields, "pojo").convertTo[String]
      val labelColumn = getOrInvalid(fields, "label_column").convertTo[String]
      val observationColumns = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      H2oModelData(modelName, pojo, labelColumn, observationColumns)
    }

    override def write(obj: H2oModelData): JsValue = {
      JsObject(
        "model_name" -> obj.modelName.toJson,
        "pojo" -> obj.pojo.toJson,
        "label_column" -> obj.labelColumn.toJson,
        "observation_columns" -> obj.observationColumns.toJson
      )
    }
  }
}
