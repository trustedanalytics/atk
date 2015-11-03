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

import javax.script.Bindings

import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.graph._
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import spray.json._

import scala.collection.JavaConversions._
import scala.util.{ Failure, Success, Try }

/**
 * Arguments for Gremlin query.
 */
case class QueryArgs(graph: GraphReference,
                     @ArgDoc("""The Gremlin script to execute.

Examples of Gremlin queries::

g.V[0..9] - Returns the first 10 vertices in graph
g.V.userId - Returns the userId property from vertices
g.V('name','hercules').out('father').out('father').name - Returns the name of Hercules' grandfather""") gremlin: String)

/**
 * Results of Gremlin query.
 *
 * The results of the Gremlin query are serialized to GraphSON (for vertices or edges) or JSON (for other results
 * like counts, property values). GraphSON is a JSON-based graph format for property graphs. GraphSON uses reserved
 * keys which begin with underscores to encode vertex and edge metadata.
 *
 * Examples of valid GraphSON:
 * { "name": "lop", "lang": "java","_id": "3", "_type": "vertex" }
 * { "weight": 1, "_id": "8", "_type": "edge", "_outV": "1",  "_inV": "4", "_label": "knows" }
 *
 * @see https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library
 *
 * @param results Results of Gremlin query serialized to JSON
 * @param run_time_seconds Runtime of Gremlin query in seconds
 */
case class QueryResult(results: Iterable[JsValue], run_time_seconds: Double)

/** Json conversion for arguments and return value case classes */
object GremlinQueryFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val queryArgsFormat = jsonFormat2(QueryArgs)
  implicit val queryResultFormat = jsonFormat2(QueryResult)
}

import GremlinQueryFormat._

/**
 * Command plugin for executing Gremlin queries.
 */
@PluginDoc(oneLine = "Executes a Gremlin query.",
  extended = """Executes a Gremlin query on an existing graph.

Notes
-----
The query does not support pagination so the results of query should be limited
using the Gremlin range filter [i..j], for example, g.V[0..9] to return the
first 10 vertices.""",
  returns = """List of query results serialized to JSON and runtime of Gremlin query in seconds.
The list of results is in GraphSON format(for vertices or edges) or JSON (for other results like counts).
GraphSON is a JSON-based format for property graphs which uses reserved keys
that begin with underscores to encode vertex and edge metadata.

Examples of valid GraphSON::

    { \"name\": \"lop\", \"lang\": \"java\",\"_id\": \"3\", \"_type\": \"vertex\" }
    { \"weight\": 1, \"_id\": \"8\", \"_type\": \"edge\", \"_outV\": \"1\", \"_inV\": \"4\", \"_label\": \"knows\" }

See https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library""")
class GremlinQueryPlugin extends CommandPlugin[QueryArgs, QueryResult] {

  val gremlinExecutor = new GremlinGroovyScriptEngine()

  /**
   * The name of the command, e.g. graph/vertex_sample
   */
  override def name: String = "graph:titan/query/gremlin"

  /**
   * Executes a Gremlin query.
   *
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments Gremlin script to execute
   * @return Results of executing Gremlin query
   */
  override def execute(arguments: QueryArgs)(implicit invocation: Invocation): QueryResult = {

    invocation.updateProgress(5f)
    val start = System.currentTimeMillis()
    val graphSONMode = GremlinUtils.getGraphSONMode(EngineConfig.config.getString("org.trustedanalytics.atk.plugins.gremlin-query.graphson-mode"))

    // TODO: cast to SparkGraphStorage shouldn't be needed
    val titanGraph = engine.graphs.asInstanceOf[SparkGraphStorage].titanGraph(arguments.graph)

    val resultIterator = Try({
      val bindings = gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)
      val results = executeGremlinQuery(titanGraph, arguments.gremlin, bindings, graphSONMode)
      results
    })
    invocation.updateProgress(100f)

    titanGraph.shutdown()

    val runtimeInSeconds = (System.currentTimeMillis() - start).toDouble / 1000.0

    val queryResult = resultIterator match {
      case Success(iterator) => QueryResult(iterator, runtimeInSeconds)
      case Failure(exception) => throw exception
    }

    queryResult
  }

  /**
   * Execute gremlin query.
   *
   * @param gremlinScript Gremlin query
   * @param bindings Bindings for Gremlin engine
   * @return Iterable of query results
   */
  def executeGremlinQuery(titanGraph: TitanGraph, gremlinScript: String,
                          bindings: Bindings,
                          graphSONMode: GraphSONMode = GraphSONMode.NORMAL): Iterable[JsValue] = {
    import org.trustedanalytics.atk.domain.DomainJsonProtocol._

    val results = Try(gremlinExecutor.eval(gremlinScript, bindings)).getOrElse({
      throw new RuntimeException(s"Invalid syntax for Gremlin query: $gremlinScript")
    })

    val resultIterator = results match {
      case x: java.lang.Iterable[_] => x.toIterable
      case x => List(x).toIterable
    }

    val jsResultsIterator = Try({
      resultIterator.filter(x => x != null).map(GremlinUtils.serializeGremlinToJson(titanGraph, _, graphSONMode))
    }).getOrElse({
      throw new RuntimeException(s"Invalid syntax for Gremlin query: $gremlinScript")
    })

    jsResultsIterator
  }
}
