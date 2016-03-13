/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.domain

import org.joda.time.DateTime

/**
 * Users of the system.
 *
 * @param id unique id auto-generated by the database
 * @param username unique name identifying this user
 * @param apiKey API key used to authenticate this user, None means user is disabled.
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class User(id: Long,
                username: Option[String],
                apiKey: Option[String],
                createdOn: DateTime,
                modifiedOn: DateTime) extends HasId {

  require(id >= 0, "id must be greater than or equal to zero")
  require(username != null, "username must not be null")
  require(apiKey != null, "api key must not be null")

  if (apiKey.isDefined) {
    require(!apiKey.get.isEmpty)
  }
}
