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

package org.trustedanalytics.atk.domain.frame.load

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Jdbc argument class
 * @param destination destination frame
 * @param tableName a non empty list of csv separated table names
 * @param url connection url*
 * @param driverName optional driver name
 * @param query optional initial query
 */
case class JdbcArgs(
    @ArgDoc("""DataFrame to load data into.Should be either a uri or id.""") destination: FrameReference,
    @ArgDoc("""table name""") tableName: String,
    @ArgDoc("""(optional) connection url (includes server name, database name, user acct and password""") url: Option[String],
    @ArgDoc("""(optional) driver name""") driverName: Option[String],
    @ArgDoc("""(optional) query for filtering. Not supported yet.""") query: Option[String] = None) {

  require(StringUtils.isNotEmpty(tableName), "table name is required")
}
