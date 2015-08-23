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
 * @param destination destination frame reference (empty frame supported)
 * @param driverType mysql, sqlserver
 * @param driverName driver name for the associated driver type
 * @param serverNameAndPort server name & (optional) port
 * @param databaseName database name
 * @param initialQuery initial sql query for data filtering
 */
case class JdbcArgs(
    @ArgDoc("""DataFrame to load data into.
Should be either a uri or id.""") destination: FrameReference,
    @ArgDoc("""jdbc driver type (mysql, postgresql, sqlserver)""") driverType: String,
    @ArgDoc("""fully qualified jdbc driver name for the supported type""") driverName: String,
    @ArgDoc("""jdbc server name and (optional) port""") serverNameAndPort: String,
    @ArgDoc("""jdbc database name""") databaseName: String,
    @ArgDoc("""optional query for filtering""") initialQuery: Option[String] = None) {

  require(StringUtils.isNotEmpty(driverType), "driver type is required")
  require(StringUtils.isNotEmpty(driverName), "driver name is required")
  require(StringUtils.isNotEmpty(serverNameAndPort), "server name is required")
  require(StringUtils.isNotEmpty(databaseName), "server name is required")
}
