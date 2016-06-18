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
package org.trustedanalytics.atk.plugins.orientdb

import com.typesafe.config.ConfigFactory

/**
 * reads the database configurations from a file
 */
object DbConfigReader extends Serializable {

  /**
   * A method to extract the database configurations from a file
   *
   * @param dbName database name
   * @return database configurations
   */

  def extractConfigurations(dbName: String): DbConfiguration = {

    val userName = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.username")
    val password = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.password")
    val rootPassword = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.rootpassword")
    val dbHost = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.host")
    val portNumber = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.port")
    val dbDir = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.url")
    val dbUri = dbDir + "/" + dbName
    DbConfiguration(dbUri, userName, password, portNumber, dbHost, rootPassword)
  }
}
