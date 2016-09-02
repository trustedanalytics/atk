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

import org.trustedanalytics.atk.scoring.ScoringServiceJsonProtocol.DataTypeJsonFormat
import org.trustedanalytics.atk.scoring.interfaces.Model
import spray.json._

import scala.collection.immutable.Map

class DataOutputFormatJsonProtocol(model: Model) {

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

  implicit object DataOutputFormat extends JsonFormat[Array[Map[String, Any]]] {

    override def write(obj: Array[Map[String, Any]]): JsValue = {
      val modelMetadata = model.modelMetadata()
      JsObject("data" -> new JsArray(obj.map(output => DataTypeJsonFormat.write(output)).toList))
    }

    //don't need this method. just there to satisfy the API.
    override def read(json: JsValue): Array[Map[String, Any]] = ???
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
