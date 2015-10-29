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


package org.trustedanalytics.atk.plugins.query

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import com.tinkerpop.blueprints.util.io.graphson._
import com.tinkerpop.blueprints.{ Element, Graph }
import com.tinkerpop.pipes.util.structures.Row
import spray.json._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Implicit conversions for Gremlin query objects to JSON
 */
object GremlinJsonProtocol extends AtkDefaultJsonProtocol with EventLogging {

  /**
   * Convert Blueprints graph elements to GraphSON.
   *
   * GraphSON is a JSON-based format for individual graph elements (i.e. vertices and edges).
   *
   * @param graph Graph used for de-serializing JSON (not needed when serializing elements to JSON)
   * @param mode GraphSON mode
   */
  class GraphSONFormat(graph: Graph = null, mode: GraphSONMode = GraphSONMode.NORMAL) extends JsonFormat[Element] {

    override def read(json: JsValue): Element = json match {
      case x if graph == null => deserializationError(s"No valid graph specified for de-serializing graph elements")
      case x if isGraphElement(x) => elementFromJson(graph, x, mode)
      case x => deserializationError(s"Expected valid GraphSON, but received: $x")
    }

    override def write(obj: Element): JsValue = obj match {
      case element: Element => {
        val jsonStr = GraphSONUtility.jsonFromElement(element, null, mode).toString()
        JsonParser(jsonStr)
      }
      case x => serializationError(s"Expected a Blueprints graph element, but received: $x")
    }
  }

  /**
   * Convert Blueprints rows to a Json.
   *
   * A Blueprints row is a list of column names and values. The row is serialized to
   * a Json Map where the column names are keys, and the column values are values.
   */
  implicit def blueprintsRowFormat[T: JsonFormat] = new JsonFormat[Row[T]] {
    override def read(json: JsValue): Row[T] = json match {
      case obj: JsObject => {
        val rowMap = obj.fields.map { field =>
          (field._1.toString, field._2.convertTo[T])
        }
        val columnNames = rowMap.keys.toList
        val columnValues = rowMap.values.toList
        new Row(columnValues, columnNames)
      }
      case x => deserializationError(s"Expected a Blueprints row, but received $x")
    }

    override def write(obj: Row[T]): JsValue = obj match {
      case row: Row[T] => {
        val obj = row.getColumnNames().map(column => {
          new JsField(column, row.getColumn(column).toJson)
        }).toMap
        obj.toJson
      }
      case x => serializationError(s"Expected a blueprints graph element, but received: $x")
    }
  }

  /**
   * Check if JSON contains a Blueprints graph element encoded in GraphSON format.
   */
  def isGraphElement(json: JsValue): Boolean = isEdge(json) | isVertex(json)

  /**
   * Check if JSON contains a Blueprints edge encoded in GraphSON format.
   */
  private def isEdge(json: JsValue): Boolean = {
    val elementType = getJsonFieldValue[String](json, GraphSONTokens._TYPE).getOrElse("")
    elementType.equalsIgnoreCase(GraphSONTokens.EDGE)
  }

  /**
   * Check if JSON contains a Blueprints vertex encoded in GraphSON format.
   */
  private def isVertex(json: JsValue): Boolean = {
    val elementType = getJsonFieldValue[String](json, GraphSONTokens._TYPE).getOrElse("")
    elementType.equalsIgnoreCase(GraphSONTokens.VERTEX)
  }

  /**
   * Create Blueprints graph element from JSON. Returns null if not a valid graph element
   */
  private def elementFromJson(graph: Graph, json: JsValue, mode: GraphSONMode = GraphSONMode.NORMAL): Element = {
    require(graph != null, "graph must not be null")
    val factory = new GraphElementFactory(graph)

    json match {
      case v if isVertex(v) => GraphSONUtility.vertexFromJson(v.toString, factory, mode, null)
      case e if isEdge(e) => {
        val inId = getJsonFieldValue[Long](e, GraphSONTokens._IN_V)
        val outId = getJsonFieldValue[Long](e, GraphSONTokens._OUT_V)
        val inVertex = if (inId.isDefined) graph.getVertex(inId.get) else null
        val outVertex = if (outId.isDefined) graph.getVertex(outId.get) else null

        if (inVertex != null && outVertex != null) {
          GraphSONUtility.edgeFromJson(e.toString, outVertex, inVertex, factory, mode, null)
        }
        else throw new RuntimeException(s"Unable to convert JSON to Blueprint's edge: $e")
      }
      case x => throw new RuntimeException(s"Unable to convert JSON to Blueprint's graph element: $x")
    }
  }

  /**
   * Get field value from JSON object using key.
   *
   * @param json Json object
   * @param key key
   * @return Optional value
   */
  private def getJsonFieldValue[T: JsonFormat: ClassTag](json: JsValue, key: String): Option[T] = json match {
    case obj: JsObject => {
      val value = obj.fields.get(key)
      value match {
        case Some(x) => Try { Some(x.convertTo[T]) }
          .getOrElse(throw new RuntimeException(s"Could not convert $key to type T from JSON string: $json"))
        case None => None
      }
    }
    case _ => None
  }
}
