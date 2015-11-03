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


package org.trustedanalytics.atk.engine.user

import org.trustedanalytics.atk.component.ClassLoaderAware
import org.trustedanalytics.atk.repository.SlickMetaStoreComponent
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.atk.domain.{ UserPrincipal, UserTemplate, User }
import org.trustedanalytics.atk.event.EventLogging
import scala.util.{ Try, Failure, Success }

/**
 * Get users from metaStore
 *
 * @param metaStore the database
 */
class UserStorage(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends EventLogging with ClassLoaderAware {

  def getUserPrincipal(apiKey: String): UserPrincipal = {
    metaStore.withSession("Getting user principal") {
      implicit session =>
        {
          if (StringUtils.isBlank(apiKey)) {
            throw new SecurityException("Api key was not provided")
          }
          val users: List[User] = metaStore.userRepo.retrieveByColumnValue("api_key", apiKey)
          users match {
            case Nil => throw new SecurityException("User not found with apiKey:" + apiKey)
            case us if us.length > 1 => throw new SecurityException("Problem accessing user credentials")
            case user => createUserPrincipalFromUser(users(0))
          }
        }
    }
  }

  def createUserPrincipalFromUser(user: User): UserPrincipal = {
    val userPrincipal: UserPrincipal = new UserPrincipal(user, List("user")) //TODO need role definitions
    userPrincipal
  }
  def insertUser(apiKey: String): UserPrincipal = {
    metaStore.withSession("Insert user") {
      implicit session =>
        {
          if (StringUtils.isBlank(apiKey)) {
            throw new SecurityException("Api key was not provided")
          }
          Try { getUserPrincipal(apiKey) } match {
            case Success(found) => throw new RuntimeException(s"Cannot insert user $apiKey because it already exists")
            case Failure(expected) => metaStore.userRepo.insert(new UserTemplate(apiKey)) match {
              case Failure(ex) => throw new RuntimeException(ex)
              case Success(user) => getUserPrincipal(apiKey)
            }
          }
        }
    }
  }
}
