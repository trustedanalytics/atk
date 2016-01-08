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

package org.trustedanalytics.atk.rest

import java.util.UUID

import org.trustedanalytics.atk.event.EventContext
import org.trustedanalytics.atk.NotFoundException
import org.trustedanalytics.atk.engine.plugin.Invocation
import spray.http.HttpHeaders.RawHeader
import spray.http.{ HttpRequest, StatusCodes }
import spray.routing._
import spray.routing.directives.LoggingMagnet

import scala.util.control.NonFatal

/**
 * Directives common to all services
 *
 * @param authenticationDirective implementation for authentication
 */
class CommonDirectives(val authenticationDirective: AuthenticationDirective) extends Directives with EventLoggingDirectives {

  val clientIdHeaderName = "Client-ID"

  def logReqResp(contextName: String)(req: HttpRequest) = {
    //In case we're re-using a thread that already had an event context
    EventContext.setCurrent(null)
    val ctx = EventContext.enter(contextName)
    info(req.method.toString() + " " + req.uri.toString())
    (res: Any) => {
      EventContext.setCurrent(ctx)
      info("RESPONSE: " + res.toString())
      ctx.close()
    }
  }

  /**
   * Directives common to all services
   * @param eventCtx name of the current context for logging
   * @return directives with authenticated user
   */
  def apply(eventCtx: String): Directive1[Invocation] = {
    //eventContext(eventCtx) &
    optionalHeaderValueByName(clientIdHeaderName).flatMap { clientIdHeader =>
      val clientId = clientIdHeader.getOrElse(UUID.randomUUID().toString)
      logRequestResponse(LoggingMagnet(logReqResp(eventCtx))) &
        //      logRequest(LoggingMagnet((req: HttpRequest) => {
        //        EventContext.enter(eventCtx)
        //        info(req.method.toString() + " " + req.uri.toString())
        //      })) &
        //      logResponse(LoggingMagnet((res: Any) => {
        //        info("RESPONSE: " + res.toString())
        //      })) &
        addCommonResponseHeaders(clientId) &
        handleExceptions(errorHandler) &
        authenticationDirective.authenticateKey(clientId)
    }
  }

  def errorHandler = {
    ExceptionHandler {
      case e: AuthenticationException =>
        complete(StatusCodes.Unauthorized)
      case e: IllegalArgumentException =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.BadRequest, "Bad request: " + e.getMessage)
      case e: NotFoundException =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.NotFound, e.getMessage)
      case NonFatal(e) =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.InternalServerError, "An internal server error occurred")
    }
  }

  /**
   * Adds header fields common to all responses
   * @return directive to wrap route with headers
   */
  def addCommonResponseHeaders(clientId: String)(): Directive0 =
    mapInnerRoute {
      route =>
        respondWithVersionAndClientId(clientId) {
          route
        }
    }

  def respondWithVersion = respondWithHeader(RawHeader("version", RestServerConfig.version))

  def respondWithVersionAndClientId(clientId: String) = respondWithHeaders(RawHeader("version", RestServerConfig.version),
    RawHeader(clientIdHeaderName, clientId))

}
