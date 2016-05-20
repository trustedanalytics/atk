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

package org.trustedanalytics.atk.domain.frame.load

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Jdbc argument class
 * @param destination destination frame
 * @param tableName table name to read from
 * @param driverName optional driver name
 */
case class LoadHdfsJdbcArgs(
    @ArgDoc("""DataFrame to load data into.Should be either a uri or id.""") destination: FrameReference,
    @ArgDoc("""table name""") tableName: String,
    @ArgDoc("""(optional) connector type""") connectorType: String = "postgres") {

  require(StringUtils.isNotEmpty(tableName), "table name is required")
  require(connectorType == "postgres" || connectorType == "mysql", "connector type must be postgres or mysql")
}
