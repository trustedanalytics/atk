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
import org.trustedanalytics.atk.scoring.interfaces.{ ModelMetaDataArgs, Model, Field }

class ScoringServiceJsonProtocol(model: Model) {

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

  implicit object DataInputFormat extends JsonFormat[Seq[Array[Any]]] {

    //don't need this method. just there to satisfy the API.
    override def write(obj: Seq[Array[Any]]): JsValue = ???

    override def read(json: JsValue): Seq[Array[Any]] = {
      val records = json.asJsObject.getFields("records") match {
        case Seq(JsArray(records)) => records
        case x => deserializationError(s"Expected array of records but got $x")
      }
      decodeRecords(records)
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
        case v: Array[_] => new JsArray(v.map {
          case d: Double => JsNumber(d)
          case n: Int => JsNumber(n)
          case n: Long => JsNumber(n)
          case n: Float => JsNumber(n)
          case s: String => JsString(s)
          case l: List[_] => new JsArray(l.map(x => write(x)))
          case m: Map[String, _] => new JsObject(m.map {
            case (a: String, l: List[Double]) => new JsField(a, l.toJson)
            case (a: String, b: Double) => new JsField(a, JsNumber(b))
            case (a: String, i: Int) => new JsField(a, JsNumber(i))
          })
          case a: Array[_] => new JsArray(a.map(x => write(x)).toList)
          case default => throw new RuntimeException("Unsupported array data type in scoring service json formatting write: " + default.getClass.getSimpleName)
        }.toList)
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
        case JsArray(v) => v.map(x => read(x))
        case obj: JsObject => obj.fields.map {
          case (a, JsArray(v)) => (a, v.map(x => read(x)))
          case (a, JsNumber(b)) => (a, b)
        }
        case unk => deserializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }
  }

  implicit object DataOutputFormat extends JsonFormat[Array[Any]] {

    override def write(obj: Array[Any]): JsValue = {
      val modelMetadata = model.modelMetadata()
      JsObject("input" -> new JsArray(model.input.map(input => FieldFormat.write(input)).toList),
        "output_columns" -> new JsArray(model.output.map(output => FieldFormat.write(output)).toList),
        "output_values" -> new JsArray(obj.map(output => DataTypeJsonFormat.write(output)).toList))
    }

    //don't need this method. just there to satisfy the API.
    override def read(json: JsValue): Array[Any] = ???
  }

  def decodeRecords(records: List[JsValue]): Seq[Array[Any]] = {
    val decodedRecords: Seq[Map[String, Any]] = records.map { record =>
      record match {
        case JsObject(fields) =>
          val decodedRecord: Map[String, Any] = for ((feature, value) <- fields) yield (feature, decodeJValue(value))
          decodedRecord
      }
    }
    var features: Seq[Array[Any]] = Seq[Array[Any]]()
    decodedRecords.foreach(decodedRecord => {
      val obsColumns = model.input()
      val featureArray = new Array[Any](obsColumns.length)
      if (decodedRecord.size != featureArray.length) {
        throw new IllegalArgumentException(
          "Size of input record is not equal to number of observation columns that model was trained on:\n" +
            s"""Expected columns are: [${obsColumns.mkString(",")}]"""
        )
      }
      decodedRecord.foreach({
        case (name, value) => {
          var counter = 0
          var found = false
          while (counter < obsColumns.length && !found) {
            if (obsColumns(counter).name != name) {
              counter = counter + 1
            }
            else {
              featureArray(counter) = value
              found = true
            }
          }
          if (!found) {
            throw new IllegalArgumentException(
              s"""$name was not found in list of Observation Columns that model was trained on: [${obsColumns.mkString(",")}]"""
            )
          }

        }
      })
      features = features :+ featureArray
    })
    features
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

}

