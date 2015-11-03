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


package org.trustedanalytics.atk.rest.v1

import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext

import scala.util.Try
import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.engine.Engine
import scala.concurrent._
import spray.http.{ StatusCodes, Uri }
import spray.routing.{ ValidationRejection, Directives, Route }
import scala.util.Failure
import scala.util.Success
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.rest.v1.viewmodels._
import org.trustedanalytics.atk.rest.v1.viewmodels.ViewModelJsonImplicits._
import org.trustedanalytics.atk.domain.command.{ CommandPost, Execution, CommandTemplate, Command }
import org.trustedanalytics.atk.rest.{ RestServerConfig, CommonDirectives }
import org.trustedanalytics.atk.rest.v1.decorators.CommandDecorator
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import org.trustedanalytics.atk.event.EventLogging

import SprayExecutionContext.global

/**
 * REST API Command Service
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
 */
class CommandService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param command The command being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, command: Command): GetCommand = {
    //TODO: add other relevant links
    val links = List(Rel.self(uri.toString()))
    CommandDecorator.decorateEntity(uri.toString(), links, command)
  }

  /**
   * The spray routes defining the command service.
   */
  def commandRoutes() = {
    commonDirectives("commands") { implicit invocation: Invocation =>
      pathPrefix("commands" / LongNumber) {
        id =>
          pathEnd {
            requestUri {
              uri =>
                get {
                  onComplete(engine.getCommand(id)) {
                    case Success(Some(command)) => complete(decorate(uri, command))
                    case Success(None) => complete(StatusCodes.NotFound)
                    case Failure(ex) => throw ex
                    case _ => reject()
                  }
                } ~
                  post {
                    entity(as[JsonTransform]) {
                      xform =>
                        {
                          val action = xform.arguments.get.convertTo[CommandPost]
                          action.status match {
                            case "cancel" => onComplete(engine.cancelCommand(id)) {
                              case Success(command) => complete("Command cancelled by client")
                              case Failure(ex) => throw ex
                            }
                          }
                        }
                    }

                  }
            }
          }
      } ~
        pathPrefix("commands") {
          path("definitions") {
            get {
              import AtkDefaultJsonProtocol.listFormat
              import DomainJsonProtocol.commandDefinitionFormat
              onComplete(Future { engine.getCommandDefinitions().toList }) {
                case Success(list) => complete(list)
                case Failure(ex) => throw ex
              }
            }
          } ~
            pathEnd {
              requestUri {
                uri =>
                  get {
                    //TODO: cursor
                    import spray.json._
                    import ViewModelJsonImplicits._
                    parameters("offset" ? 0, "count" ? RestServerConfig.defaultCount) { (offset, count) =>
                      onComplete(engine.getCommands(offset, count)) {
                        case Success(commands) => complete(CommandDecorator.decorateForIndex(uri.withQuery().toString(), commands))
                        case Failure(ex) => throw ex
                      }
                    }
                  } ~
                    post {
                      entity(as[JsonTransform]) {
                        xform =>
                          val template = CommandTemplate(name = xform.name, arguments = xform.arguments)
                          info(s"Received command template for execution: $template")
                          onComplete(Future { engine.execute(template) }) {
                            case Success(Execution(command, futureResult)) => complete(decorate(uri + "/" + command.id, command))
                            case Failure(e: DeserializationException) =>
                              val message = s"Incorrectly formatted JSON found while parsing command '${xform.name}':" +
                                s" ${e.getMessage}"
                              failWith(new DeserializationException(message, e))
                            case Failure(ex) => throw ex
                          }
                      }
                    }
              }

            }
        }
    }
  }

  //TODO: internationalization
  def getErrorMessage[T](value: Try[T]): String = value match {
    case Success(x) => ""
    case Failure(ex) => ex.getMessage
  }
}
