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


package org.trustedanalytics.atk.testutils

import breeze.linalg.DenseMatrix
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens
import com.tinkerpop.blueprints.{ Direction, Edge, Vertex }
import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }
import spray.json._

import scala.collection.JavaConverters._

object MatcherUtils extends Matchers {

  /**
   * Tests if two arrays of Double are equal +- tolerance.
   *
   * <pre class="stHighlight">
   * Array(0.12, 0.25) should  equalWithTolerance(Array(0.122, 0.254), 0.01)
   * </pre>
   */
  def equalWithTolerance(right: Array[Double], tolerance: Double = 1E-6) =
    Matcher { (left: Array[Double]) =>
      MatchResult(
        (left zip right) forall { case (a, b) => a === (b +- tolerance) },
        left.deep.mkString(" ") + " did not equal " + right.deep.mkString(" ") + " with tolerance " + tolerance,
        left.deep.mkString(" ") + " equaled " + right.deep.mkString(" ") + " with tolerance " + tolerance
      )
    }

  /**
   * Tests if two Scala Breeze dense matrices are equal +- tolerance.
   *
   * <pre class="stHighlight">
   * DenseMatrix((1330d, 480d)) should  equalWithTolerance(DenseMatrix((1330.02, 480d.09)), 0.1)
   * </pre>
   */
  def equalWithToleranceMatrix(right: DenseMatrix[Double], tolerance: Double = 1E-6) =
    Matcher { (left: DenseMatrix[Double]) =>
      MatchResult(
        if (left.size === right.size) {
          val results = for {
            i <- 0 until right.rows
            j <- 0 until right.cols
            r = left(i, j) === (right(i, j) +- tolerance)
          } yield r
          results forall (x => x)
        }
        else false,
        left.toString() + " did not equal " + right.toString() + " with tolerance " + tolerance,
        left.toString() + " equaled " + right.toString() + " with tolerance " + tolerance
      )
    }

  /**
   * Tests if the GraphSON representation of the Blueprint's vertex is valid
   *
   * <pre class="stHighlight">
   * "{"name":"marko", "age":29, "_id":10, "_type":"vertex" }" should  equalsBlueprintsVertex(vertex)
   * </pre>
   */
  def equalsGraphSONVertex(json: JsValue) =
    Matcher { (vertex: Vertex) =>
      MatchResult(matchGraphSONVertex(vertex, json),
        json + " does not equal the GraphSON representation for " + vertex,
        json + " equals the GraphSON representation for " + vertex
      )
    }

  /**
   * Tests if the GraphSON representation of the Blueprint's vertex is valid
   *
   * <pre class="stHighlight">
   * {"weight":0.5,"_id":7,"_type":"edge","_outV":1,"_inV":2,"_label":"knows"}" should  equalsBlueprintsEdge(edge)
   * </pre>
   */
  def equalsGraphSONEdge(json: JsValue) =
    Matcher { (edge: Edge) =>
      MatchResult(matchGraphSONEdge(edge, json),
        json + " does not equal the GraphSON representation for " + edge,
        json + " equals the GraphSON representation for " + edge
      )
    }

  /**
   * Returns true if JSON is a valid GraphSON representation of vertex
   */
  private def matchGraphSONVertex(vertex: Vertex, json: JsValue): Boolean = {
    getJsonFieldValue(json, GraphSONTokens._ID) === vertex.getId &&
      getJsonFieldValue(json, GraphSONTokens._TYPE) === GraphSONTokens.VERTEX &&
      (vertex.getPropertyKeys.asScala forall {
        case a => vertex.getProperty(a).toString === getJsonFieldValue(json, a).toString
      })
  }

  /**
   * Returns true if JSON is a valid GraphSON representation of edge
   */
  private def matchGraphSONEdge(edge: Edge, json: JsValue): Boolean = {
    getJsonFieldValue(json, GraphSONTokens._TYPE) === GraphSONTokens.EDGE &&
      getJsonFieldValue(json, GraphSONTokens._IN_V) === edge.getVertex(Direction.IN).getId &&
      getJsonFieldValue(json, GraphSONTokens._OUT_V) === edge.getVertex(Direction.OUT).getId &&
      getJsonFieldValue(json, GraphSONTokens._LABEL) === edge.getLabel &&
      (edge.getPropertyKeys.asScala forall {
        case a => edge.getProperty(a).toString === getJsonFieldValue(json, a).toString
      })
  }

  /**
   * Get field value from JSON object using key, and convert value to a Scala object
   */
  private def getJsonFieldValue(json: JsValue, key: String): Any = json match {
    case obj: JsObject => {
      val value = obj.fields.get(key).orNull
      value match {
        case x: JsBoolean => x.value
        case x: JsNumber => x.value
        case x: JsString => x.value
        case x => x.toString
      }
    }
    case x => x.toString()
  }

}
