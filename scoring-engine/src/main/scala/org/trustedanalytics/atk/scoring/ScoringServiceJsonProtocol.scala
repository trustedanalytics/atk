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

import org.codehaus.jettison.json.JSONObject
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer
import org.trustedanalytics.atk.scoring.interfaces.{ Model, Field }

class ScoringServiceJsonProtocol(model: Model) {

  //  implicit object MetadataFormat extends JsonFormat[Array[Any]] {
  //    override def write(obj: Array[Any]): JsValue = {
  //      JsObject(
  //        "name" -> JsString(obj.name),
  //        "value" -> JsString(obj.value))
  //    }
  //
  //    override def read(json: JsValue): MetaField = {
  //      val fields = json.asJsObject.fields
  //      val name = fields.get("name").get.asInstanceOf[JsString].value.toString
  //      val value = fields.get("value").get.asInstanceOf[JsString].value.toString
  //
  //      MetaField(name, value)
  //    }
  //  }

  implicit object FieldFormat extends JsonFormat[Field] {
    override def write(obj: Field): JsValue = {
      JsObject(
        "name" -> JsString(obj.name),
        "value" -> JsString(obj.dataType))
    }

    override def read(json: JsValue): Field = {
      val fields = json.asJsObject.fields
      val name = fields.get("name").get.asInstanceOf[JsString].value.toString
      val value = fields.get("dataType").get.asInstanceOf[JsString].value.toString

      Field(name, value)
    }
  }

  implicit object DataInputFormat extends JsonFormat[Seq[Array[Any]]] {

    //don't need this method. just there to satisfy the API.
    override def write(obj: Seq[Array[Any]]): JsValue = {
      val jsObject = JsObject()
      jsObject

    }

    override def read(json: JsValue): Seq[Array[Any]] = {
      val records = json.asJsObject.getFields("records") match {
        case Seq(JsArray(records)) => records
        case x => deserializationError(s"Expected array of records but got $x")
      }

      val decodedRecords: Seq[Map[String, Any]] = records.map { record =>
        record match {
          case JsObject(fields) =>
            val decodedRecord: Map[String, Any] = for ((feature, value) <- fields) yield (feature, decodeJValue(value))
            decodedRecord
        }
      }

      var features: Seq[Array[Any]] = Seq[Array[Any]]()

      decodedRecords.foreach(decodedRecord => {
        val featureArray = new Array[Any](model.input().length)
        val obsColumns = model.input()
        decodedRecord.foreach(record => {
          var counter = 0
          while (obsColumns(counter).name != record._1 && counter < obsColumns.length) {
            counter = counter + 1
          }
          featureArray(counter) = record._2
        })
        features = features :+ featureArray
      })
      features
    }
  }

  def decodeJValue(v: JsValue): Any = {
    v match {
      case JsString(s) => s
      case JsNumber(n) => n.toDouble
      case JsArray(items) => for (item <- items) yield decodeJValue(item)
      case JsNull => null
      case JsObject(fields) =>
        val decodedValue: Map[String, Any] = for ((feature, value) <- fields) yield (feature, decodeJValue(value))
        decodedValue
      case x => deserializationError(s"Unexpected JSON type in record $x")
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
        case v: Array[_] => new JsArray(v.map { case d: Double => JsNumber(d) }.toList)
        case v: ArrayBuffer[_] => new JsArray(v.map { case d: Double => JsNumber(d) }.toList) // for vector DataType
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
        case JsArray(v) => v.map(x => read(x)).toList
        case unk => deserializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }
  }

  implicit object DataOutputFormat extends JsonFormat[Array[Any]] {

    override def write(obj: Array[Any]): JsValue = {
      val modelMetadata = model.modelMetadata()
      println("about to serialize")
      //JsObject("Model Details" -> new JsArray(modelMetadata.map(data => DataTypeJsonFormat.write(data)).toList),
      JsObject("Model Details" -> new JsArray(List(DataTypeJsonFormat.write(modelMetadata))),
        "Input" -> new JsArray(model.input.map(input => FieldFormat.write(input)).toList),
        "output" -> new JsArray(obj.map(output => DataTypeJsonFormat.write(output)).toList))
    }

    //don't need this method. just there to satisfy the API.
    override def read(json: JsValue): Array[Any] = {
      val outputArray = new Array[Any](model.output().length)
      outputArray
    }
  }
}

