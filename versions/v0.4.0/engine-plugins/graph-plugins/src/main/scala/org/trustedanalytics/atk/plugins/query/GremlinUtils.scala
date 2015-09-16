/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.plugins.query

import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.blueprints.{ Element, Graph }
import spray.json._

import scala.reflect.ClassTag
import scala.util.Try

object GremlinUtils {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._ // Needed for serializing Java collections to JSON
  import org.trustedanalytics.atk.plugins.query.GremlinJsonProtocol._

  /**
   * Serializes results of Gremlin query to JSON.
   *
   * @param graph Blueprint's graph
   * @param obj Results of Gremlin query
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Serialized query results
   */
  def serializeGremlinToJson[T: JsonFormat: ClassTag](graph: Graph,
                                                      obj: T,
                                                      mode: GraphSONMode = GraphSONMode.NORMAL): JsValue = {

    implicit val graphSONFormat = new GraphSONFormat(graph)

    val json = obj match {
      case null => JsNull
      case blueprintElement: Element => blueprintElement.toJson
      case s => Try(s.toJson).getOrElse(JsString(s.toString))
    }

    json
  }

  /**
   * Deserializes JSON into a Scala object.
   *
   * @param graph Blueprint's graph
   * @param json Json objects
   * @param mode GraphSON mode which can be either normal, compact or extended
   *
   * @return Deserialized query results
   */
  def deserializeJsonToGremlin[T: JsonFormat: ClassTag](graph: Graph,
                                                        json: JsValue,
                                                        mode: GraphSONMode = GraphSONMode.NORMAL): T = {
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val obj = json match {
      case x if isGraphElement(x) => graphSONFormat.read(json).asInstanceOf[T]
      case x => x.convertTo[T]
    }
    obj
  }

  /**
   * Get the GraphSON mode type from a string.
   *
   * @param name Name of GraphSON mode. Supported names are: "normal", "compact", and "extended".
   * @return GraphSON mode type (defaults to GraphSONMode.NORMAL)
   */
  def getGraphSONMode(name: String): GraphSONMode = name match {
    case "normal" => GraphSONMode.NORMAL
    case "compact" => GraphSONMode.COMPACT
    case "extended" => GraphSONMode.EXTENDED
    case x => throw new IllegalArgumentException(s"Unsupported GraphSON mode: $x. " +
      "Supported values are: normal, compact, and extended.")
  }
}
