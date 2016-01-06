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

//TODO: Is this right execution context for us?

import java.util.concurrent.{ Callable, TimeUnit }

import com.google.common.cache.CacheBuilder
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.UserPrincipal
import org.trustedanalytics.atk.engine.plugin.{ Invocation, Call }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import spray.http.HttpHeader

import scala.PartialFunction._
import scala.concurrent._
import spray.routing._
import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.event.EventLogging
import scala.util.{ Failure, Success, Try }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext.global

import scala.util.parsing.json.JSON

/**
 * Uses authorization HTTP header and engine to authenticate a user
 */
class AuthenticationDirective(val engine: Engine) extends Directives with EventLogging with EventLoggingImplicits {

  private lazy val shortCircuitApiKey = RestServerConfig.shortCircuitApiKey

  /**
   * Caches user principals so that they don't have to be looked up every time.
   *
   * (This was originally added for QA parallel testing)
   */
  private lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(RestServerConfig.userPrincipalCacheTimeoutSeconds, TimeUnit.SECONDS)
    .maximumSize(RestServerConfig.userPrincipalCacheMaxSize)
    .build[String, UserPrincipal]()

  // TODO: clientId is needed in Call

  /**
   * Gets authorization header and authenticates a user
   * @return the authenticated user
   */
  def authenticateKey: Directive1[Invocation] =
    //TODO: proper authorization with spray authenticate directive in a manner similar to S3.
    optionalHeaderValue(getUserPrincipalFromHeader).flatMap {
      case Some(p) => provide(Call(p, SprayExecutionContext.global, "deleteme"))
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, List()))
    }

  protected def getUserPrincipalFromHeader(header: HttpHeader): Option[UserPrincipal] =
    condOpt(header) {
      case h if h.is("authorization") => getUserPrincipal(h.value)
    }

  protected def getUserPrincipal(apiKey: String): UserPrincipal = {
    cache.get(apiKey, new Callable[UserPrincipal]() {
      override def call(): UserPrincipal = {
        // cache miss, look it up
        Await.result(lookupUserPrincipal(apiKey)(Call(null, SprayExecutionContext.global, null)), RestServerConfig.defaultTimeout)
      }
    })
  }

  protected def lookupUserPrincipal(apiKey: String)(implicit invocation: Invocation): Future[UserPrincipal] = {
    withContext("AuthenticationDirective") {
      if (StringUtils.isBlank(apiKey)) {
        warn("Api key was not provided")
        throw new SecurityException("Api key was not provided")
      }
      future {
        if (apiKey.equals(shortCircuitApiKey)) {
          val userPrincipal = engine.getUserPrincipal(shortCircuitApiKey)
          info("authenticated " + userPrincipal)
          userPrincipal
        }
        else {
          val tokenUserInfo = CfRequests.getTokenUserInfo(apiKey)

          // todo - add mapping support from userId to userName for humans
          val userId = tokenUserInfo.userId
          val userName = tokenUserInfo.userName
          val appOrganizationId = try {
            CfRequests.getOrganizationForSpaceId(apiKey, RestServerConfig.appSpace)
          }
          catch {
            case ex: Throwable =>
              error(ex.getMessage)
              throw new AuthenticationException("CF-InvalidAuthToken", ex.getCause)
          }
          val userPrincipal: UserPrincipal = Try {
            engine.getUserPrincipal(userId)
          } match {
            case Success(found) => found
            case Failure(missing) =>
              // Don't know about this user id.  See if the user meets requirements to be added to the metastore
              // 1. The userId must belong to the same organization as this server instance
              val userOrganizationIds = CfRequests.getOrganizationsForUserId(apiKey, userId)
              if (userOrganizationIds.contains(appOrganizationId)) {
                engine.addUserPrincipal(userId)
              }
              else {
                throw new RuntimeException(s"User $userId ($userName) is not a member of this server's organization")
              }
          }
          info("authenticated " + userPrincipal)
          userPrincipal.copy(token = Some(apiKey), appOrgId = Some(appOrganizationId))
        }
      }
    }
  }
}
