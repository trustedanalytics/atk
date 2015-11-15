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

package org.trustedanalytics.atk.repository.util

// $COVERAGE-OFF$
// This is a developer utility, not part of the main product

import java.sql.DriverManager

/**
 * Developer utility for testing JDBC Connectivity
 */
object JdbcConnectionPing {

  // URL Format: "jdbc:postgresql://host:port/database"
  var url = "jdbc:postgresql://localhost:5432/metastore"
  var user = "metastore"
  var password = "Trusted123"

  /**
   * Establish JDBC Connection, read now() from database
   * @param args url, user, password
   */
  def main(args: Array[String]) {

    if (args.length == 3) {
      url = args(1)
      user = args(2)
      password = args(3)
    }

    println("Trying to Connect (url: " + url + ", user: " + user + ", password: " + password.replaceAll(".", "*") + ")")

    val connection = DriverManager.getConnection(url, user, password)
    val resultSet = connection.createStatement().executeQuery("SELECT now()")
    while (resultSet.next()) {
      println("SELECT now() = " + resultSet.getString(1))
      println("Test was SUCCESSFUL")
    }

  }
}
