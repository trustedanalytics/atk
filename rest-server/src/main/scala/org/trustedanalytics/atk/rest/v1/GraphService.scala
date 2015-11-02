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

package org.trustedanalytics.atk.rest.v1

import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import spray.http.{ StatusCodes, Uri }
import org.trustedanalytics.atk.engine.Engine
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util._
import org.trustedanalytics.atk.rest.v1.viewmodels.GetGraph
import org.trustedanalytics.atk.domain.graph.{ SeamlessGraphMeta, GraphTemplate, GraphEntity }
import org.trustedanalytics.atk.rest.CommonDirectives
import spray.routing.Directives
import org.trustedanalytics.atk.rest.v1.decorators.{ GetGraphComponent, DecorateReadyGraphEntity, FrameDecorator, GraphDecorator }

import org.trustedanalytics.atk.rest.v1.viewmodels.ViewModelJsonImplicits
import org.trustedanalytics.atk.rest.v1.viewmodels.Rel
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import org.trustedanalytics.atk.event.EventLogging
import spray.json._

import SprayExecutionContext.global

/**
 * REST API Graph Service.
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
 */
class GraphService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Graph service.
   */
  def graphRoutes() = {
    val prefix = "graphs"
    val verticesPrefix = "vertices"
    val edgesPrefix = "edges"

    /**
     * Creates an graph object ready for decoration
     * @param graph the graph entity
     * @param invocation implicit context
     * @return
     */
    def getDecorateReadyGraphEntity(graph: GraphEntity)(implicit invocation: Invocation): DecorateReadyGraphEntity = {
      if (graph.isSeamless) {
        val vertices: List[FrameEntity] = Await.result(engine.getVertices(graph.id), Duration.Inf).toList
        val edges: List[FrameEntity] = Await.result(engine.getEdges(graph.id), Duration.Inf).toList
        val seamless = SeamlessGraphMeta(graph, vertices ++ edges)
        val vertexComponents = seamless.vertexFramesMap.map { case (label: String, frame: FrameEntity) => GetGraphComponent(label, frame.rowCount.getOrElse(0), frame.schema.columnNames.filter(name => !name.startsWith("_"))) }.toList
        val edgeComponents = seamless.edgeFramesMap.map { case (label: String, frame: FrameEntity) => GetGraphComponent(label, frame.rowCount.getOrElse(0), frame.schema.columnNames.filter(name => !name.startsWith("_"))) }.toList
        DecorateReadyGraphEntity(graph, vertexComponents, edgeComponents)
      }
      else {
        DecorateReadyGraphEntity(graph, Nil, Nil)
      }
    }

    /**
     * Creates "decorated graph" for return on HTTP protocol
     * @param uri handle of graph
     * @param graph graph metadata
     * @return Decorated graph for HTTP protocol return
     */
    def decorate(uri: Uri, graph: GraphEntity)(implicit invocation: Invocation): GetGraph = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      val meta = getDecorateReadyGraphEntity(graph)
      GraphDecorator.decorateEntity(uri.toString, links, meta)
    }

    //TODO: none of these are yet asynchronous - they communicate with the engine
    //using futures, but they keep the client on the phone the whole time while they're waiting
    //for the engine work to complete. Needs to be updated to a) register running jobs in the metastore
    //so they can be queried, and b) support the web hooks.
    // note: delete is partly asynchronous - it wraps its backend delete in a future but blocks on deleting the graph
    // from the metastore

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        (path(prefix) & pathEnd) {
          requestUri {
            uri =>
              get {
                parameters('name.?) {
                  import spray.httpx.SprayJsonSupport._
                  implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                  (name) => name match {
                    case Some(name) => {
                      onComplete(engine.getGraphByName(name)) {
                        case Success(Some(graph)) => {
                          val links = List(Rel.self(uri.toString))
                          complete(GraphDecorator.decorateEntity(uri.toString(), links, getDecorateReadyGraphEntity(graph)))
                        }
                        case Success(None) => complete(StatusCodes.NotFound, s"Graph with name '$name' was not found.")
                        case Failure(ex) => throw ex
                      }
                    }
                    case _ =>
                      //TODO: cursor
                      onComplete(engine.getGraphs()) {
                        case Success(graphs) =>
                          import AtkDefaultJsonProtocol._
                          implicit val indexFormat = ViewModelJsonImplicits.getGraphsFormat
                          complete(GraphDecorator.decorateForIndex(uri.toString(), graphs.map(getDecorateReadyGraphEntity(_))))
                        case Failure(ex) => throw ex
                      }
                  }
                }
              } ~
                post {
                  import spray.httpx.SprayJsonSupport._
                  implicit val format = DomainJsonProtocol.graphTemplateFormat
                  implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                  entity(as[GraphTemplate]) {
                    graph =>
                      onComplete(engine.createGraph(graph)) {
                        case Success(graph) => complete(decorate(uri + "/" + graph.id, graph))
                        case Failure(ex) => ctx => {
                          ctx.complete(500, ex.getMessage)
                        }
                      }
                  }
                }
          }
        } ~
          pathPrefix(prefix / LongNumber) {
            id =>
              {
                pathEnd {
                  requestUri {
                    uri =>
                      get {
                        onComplete(engine.getGraph(id)) {
                          case Success(graph) => {
                            val decorated = decorate(uri, graph)
                            complete {
                              import spray.httpx.SprayJsonSupport._
                              implicit val format = DomainJsonProtocol.graphTemplateFormat
                              implicit val indexFormat = ViewModelJsonImplicits.getGraphFormat
                              decorated
                            }
                          }
                          case Failure(ex) => throw ex
                        }
                      } ~
                        delete {
                          onComplete(engine.dropGraph(id)) {
                            case Success(ok) => complete("OK")
                            case Failure(ex) => throw ex
                          }
                        }
                  }
                } ~
                  pathPrefix(verticesPrefix) {
                    pathEnd {
                      requestUri {
                        uri =>
                          {
                            get {
                              import AtkDefaultJsonProtocol._
                              parameters('label.?) {
                                (label) =>
                                  label match {
                                    case Some(label) => {
                                      onComplete(engine.getVertex(id, label)) {
                                        case Success(frame) => {
                                          import spray.httpx.SprayJsonSupport._
                                          import ViewModelJsonImplicits.getFrameFormat
                                          complete(FrameDecorator.decorateEntity(uri.toString, Nil, frame))
                                        }
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                    case None => {
                                      onComplete(engine.getVertices(id)) {
                                        case Success(frames) =>
                                          import spray.httpx.SprayJsonSupport._
                                          import AtkDefaultJsonProtocol._
                                          import ViewModelJsonImplicits.getFrameFormat
                                          complete(FrameDecorator.decorateEntities(uri.toString(), Nil, frames))
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                  }
                              }
                            }
                          }
                      }
                    }
                  } ~
                  pathPrefix(edgesPrefix) {
                    pathEnd {
                      requestUri {
                        uri =>
                          {
                            get {
                              import AtkDefaultJsonProtocol._
                              parameters('label.?) {
                                (label) =>
                                  label match {
                                    case Some(label) => {
                                      onComplete(engine.getEdge(id, label)) {
                                        case Success(frame) => {
                                          import spray.httpx.SprayJsonSupport._
                                          import ViewModelJsonImplicits.getFrameFormat
                                          complete(FrameDecorator.decorateEntity(uri.toString, Nil, frame))
                                        }
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                    case None => {
                                      onComplete(engine.getEdges(id)) {
                                        case Success(frames) =>
                                          import spray.httpx.SprayJsonSupport._
                                          import AtkDefaultJsonProtocol._
                                          import ViewModelJsonImplicits.getFrameFormat
                                          complete(FrameDecorator.decorateEntities(uri.toString(), Nil, frames))
                                        case Failure(ex) => throw ex
                                      }
                                    }
                                  }
                              }
                            }
                          }
                      }
                    }
                  }

              }
          }
    }
  }
}
