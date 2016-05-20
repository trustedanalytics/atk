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

package org.trustedanalytics.atk.engine.frame.plugins.load

import java.util.Properties
import com.typesafe.config.ConfigFactory

/**
 * Helper class to pass relevant information from the config to the jdbc connection
 */
case class JdbcConnectionInfo(connector: String) {

  def userPassProperties() = {
    val properties = new Properties()
    properties.setProperty("username", ConfigFactory.load().getString("trustedanalytics.atk.datastore." + connector + ".username"))
    properties.setProperty("password", ConfigFactory.load().getString("trustedanalytics.atk.datastore." + connector + ".password"))
    properties
  }

  def urlString() = {
    ConfigFactory.load().getString("trustedanalytics.atk.datastore." + connector + ".url")
  }
}

/**
 * Helper class for creating an RDD from jdbc
 */
object JdbcFunctions extends Serializable {

  /**
   * Builds connection url for cluster/cloud deployment.
   * @return a connection url, the username, and the password
   */
  def buildUrl(connectorType: String) = {
    val connector = connectorType match {
      case "postgres" => "connection-postgres"
      case "mysql" => "connection-mysql"
      case _ => throw new IllegalArgumentException("value must be postgres or mysql")
    }
    JdbcConnectionInfo(connector)

  }
}
