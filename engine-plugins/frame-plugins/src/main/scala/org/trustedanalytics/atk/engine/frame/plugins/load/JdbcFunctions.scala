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

package org.trustedanalytics.atk.engine.frame.plugins.load

/**
 * Helper class for creating an RDD from jdbc
 */
object JdbcFunctions extends Serializable {

  /**
   * Builds connection argmuments for jdbc
   * @param tableName table name
   * @param url optional connection url
   * @param driverName optional driver name
   * @return connection args as map
   */
  def buildConnectionArgs(tableName: String, url: Option[String], driverName: Option[String]): Map[String, String] = {
    val connectionUrl = url.getOrElse(buildUrl())

    if (driverName.isEmpty) {
      Map(
        urlKey -> connectionUrl,
        dbTableKey -> tableName)
    }
    else {
      Map(
        urlKey -> connectionUrl,
        dbTableKey -> tableName,
        "driver" -> driverName.get)
    }
  }

  /**
   * url key used for connection map
   * @return "url"
   */
  def urlKey = "url"

  /**
   * table key used for connection map
   * @return "dbtable"
   */
  def dbTableKey = "dbtable"

  /**
   * Builds connection url for cluster/cloud deployment. Not supported yet.
   * @return a connection url
   */
  private def buildUrl(): String = {
    throw new IllegalArgumentException("Connection url is required")
  }
}
