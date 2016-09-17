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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import hex.tree.drf.DRFModel
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.mllib.atk.plugins.InvalidJsonException
import spray.json._

import scala.util.Try

object H2oJsonProtocol {

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

  implicit object DRFModelModelFormat extends JsonFormat[DRFModel] {
    override def read(json: JsValue): DRFModel = {
      val fields = json.asJsObject.fields
      val modelKey = getOrInvalid(fields, "model_key")
      val jsonModel = getOrInvalid(fields, "serialized_model")
      val serializedModel = jsonModel.convertTo[Array[Byte]]
      val objectInput = new ObjectInputStream(new ByteArrayInputStream(serializedModel))
      val drfModel = Try {
        objectInput.readObject().asInstanceOf[DRFModel]
      }.getOrElse({
        throw new IllegalArgumentException("Could not convert serialized model to H2o random forest model")
      })
      drfModel
    }

    override def write(obj: DRFModel): JsValue = {
      val byteOutput = new ByteArrayOutputStream()
      val objectOutput = new ObjectOutputStream(byteOutput)
      obj.writeExternal(objectOutput)
      val serializedModel = byteOutput.toByteArray
      objectOutput.close()
      JsObject("model_key" -> obj._key.toString.toJson, "serialized_model" -> serializedModel.toJson)
    }
  }

}
