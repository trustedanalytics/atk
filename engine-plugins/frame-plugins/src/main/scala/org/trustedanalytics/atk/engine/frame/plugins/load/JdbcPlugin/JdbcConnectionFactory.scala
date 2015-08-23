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

package org.trustedanalytics.atk.engine.frame.plugins.load.JdbcPlugin

import java.sql.{ DriverManager, Connection }
import org.trustedanalytics.atk.domain.frame.load.JdbcArgs

/**
 * Jdbc factory for connections and connections strings
 */
object JdbcConnectionFactory extends Serializable {

  /**
   * Create a new connection to database
   * @param arguments connection required arguments
   * @param user user
   * @param password user password
   * @return a jdbc connection
   */
  def createConnection(arguments: JdbcArgs, user: String, password: String): Connection = {

    //load the driver
    Class.forName(arguments.driverName)
    DriverManager.getConnection(connectionUrl(arguments), user, password)
  }

  /**
   * Create a connection url string for jdbc
   * @param arguments required arguments
   * @return a connection url
   */
  def connectionUrl(arguments: JdbcArgs): String = {

    var url: String = "jdbc:"

    //connectionUrl
    if (isMySql(arguments.driverType)) {
      url += createMySqlConnectionUrl(arguments)
    }
    else if (isSqlServer(arguments.driverType)) {
      url += createSqlServerConnectionUrl(arguments)
    }
    else {
      throw new IllegalArgumentException(s"unsupported driver type ${arguments.driverType}")
    }

    url
  }

  /**
   * Checks if the driver type is my sql
   * @param driverType driver type
   * @return true if the driver is mySql; false otherwise
   */
  private def isMySql(driverType: String) = {
    "mysql".equalsIgnoreCase(driverType)
  }

  /**
   * Checks if the driver type is sql server
   * @param driverType driver type
   * @return true if the driver is sel server; false otherwise
   */
  private def isSqlServer(driverType: String) = {
    "sqlserver".equalsIgnoreCase(driverType)
  }

  /**
   * Creates a mySql connection string
   * @param arguments required arguments
   * @return the connection string
   */
  private def createMySqlConnectionUrl(arguments: JdbcArgs): String = {
    arguments.driverType + "://" + arguments.serverNameAndPort + "/" + arguments.databaseName
  }

  /**
   * Creates a sql server connection string
   * @param arguments required arguments
   * @return the connection string
   */
  private def createSqlServerConnectionUrl(arguments: JdbcArgs): String = {
    arguments.driverType + "://" + arguments.serverNameAndPort + ";database=" + arguments.databaseName
  }
}
