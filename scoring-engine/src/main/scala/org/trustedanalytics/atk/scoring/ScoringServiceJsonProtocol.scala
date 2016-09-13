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

package org.trustedanalytics.atk.scoring

import org.joda.time.DateTime
import org.trustedanalytics.atk.scoring.interfaces.{ Field, ModelMetaDataArgs }
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

object ScoringServiceJsonProtocol {

  implicit object ModelMetaDataFormat extends JsonFormat[ModelMetaDataArgs] {
    override def write(obj: ModelMetaDataArgs): JsValue = {
      JsObject(
        "model_type" -> JsString(obj.modelType),
        "model_class" -> JsString(obj.modelClass),
        "model_reader" -> JsString(obj.modelReader),
        "custom_values" -> obj.customMetaData.toJson)
    }

    override def read(json: JsValue): ModelMetaDataArgs = ???
  }

  implicit object FieldFormat extends JsonFormat[Field] {
    override def write(obj: Field): JsValue = {
      JsObject(
        "name" -> JsString(obj.name),
        "value" -> JsString(obj.dataType))
    }

    override def read(json: JsValue): Field = {
      val fields = json.asJsObject.fields
      val name = fields.get("name").get.asInstanceOf[JsString].value.toString
      val value = fields.get("data_type").get.asInstanceOf[JsString].value.toString

      Field(name, value)
    }
  }

  implicit object DataTypeJsonFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      obj match {
        case n: Int => new JsNumber(n)
        case n: Long => new JsNumber(n)
        case n: Float => new JsNumber(BigDecimal(n))
        case n: Double => new JsNumber(n)
        case s: String => new JsString(s)
        case s: Boolean => JsBoolean(s)
        case dt: DateTime => JsString(org.joda.time.format.ISODateTimeFormat.dateTime.print(dt))
        case m: Map[_, _] @unchecked => mapToJson(m)
        case v: List[_] => listToJson(v)
        case v: Array[_] => listToJson(v.toList)
        case v: Vector[_] => listToJson(v.toList)
        case v: ArrayBuffer[_] @unchecked => listToJson(v.toList)
        case n: java.lang.Long => new JsNumber(n.longValue())
        // case null => JsNull  Consciously not writing nulls, may need to change, but for now it may catch bugs
        case unk =>
          val name: String = if (unk != null) {
            unk.getClass.getName
          }
          else {
            "null"
          }
          serializationError("Cannot serialize " + name)
      }
    }

    override def read(json: JsValue): Any = {
      json match {
        case JsNumber(n) if n.isValidInt => n.intValue()
        case JsNumber(n) if n.isValidLong => n.longValue()
        case JsNumber(n) if n.isValidFloat => n.floatValue()
        case JsNumber(n) => n.doubleValue()
        case JsBoolean(b) => b
        case JsString(s) => s
        case JsArray(v) => v.map(x => read(x))
        case obj: JsObject => obj.fields.map {
          case (a, JsArray(v)) => (a, v.map(x => read(x)))
          case (a, JsNumber(b)) => (a, b)
        }
        case unk => deserializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }
  }

  private def mapToJson[K <: Any, V <: Any](m: Map[K, V]): JsObject = {
    require(m != null, s"Scoring service cannot serialize null to JSON")
    val jsMap: Map[String, JsValue] = m.map {
      case (x) => x match {
        case (k: String, n: Double) => (k, n.toJson)
        case (k: String, n: Int) => (k, n.toJson)
        case (k: String, n: Long) => (k, n.toJson)
        case (k: String, n: Float) => (k, n.toJson)
        case (k: String, str: String) => (k, JsString(str))
        case (k: String, list: List[_]) => (k, listToJson(list))
        case (k: String, array: Array[_]) => (k, listToJson(array.toList))
        case (k: String, vector: Vector[_]) => (k, listToJson(vector.toList))
        case unk => serializationError(s"Scoring service cannot serialize ${unk.getClass.getName} to JSON")
      }
    }
    JsObject(jsMap)
  }

  private def listToJson(list: List[Any]): JsArray = {
    require(list != null, s"Scoring service cannot serialize null to JSON")
    val jsElements = list.map {
      case n: Double => n.toJson
      case n: Int => n.toJson
      case n: Long => n.toJson
      case n: Float => n.toJson
      case str: String => str.toJson
      case map: Map[_, _] @unchecked => mapToJson(map)
      case list: List[_] => listToJson(list)
      case arr: Array[_] => listToJson(arr.toList)
      case vector: Vector[_] => listToJson(vector.toList)
      case unk => serializationError(s"Scoring service cannot serialize ${unk.getClass.getName} to Json")
    }
    new JsArray(jsElements)
  }
}

