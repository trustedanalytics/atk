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

package org.trustedanalytics.atk.engine.frame.plugins.load.JdbcPlugin

import java.sql.DriverManager
import java.util.Properties
import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.trustedanalytics.atk.domain.frame.load.{ JdbcArgs }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.engine.frame.plugins.load.JdbcFunctions

/**
 * Helper class for creating an RDD from jdbc
 */
object LoadJdbcImpl extends Serializable {

  /**
   * Create a data frame from an jdbc compatible database
   * @param sc default spark context
   * @param arguments arguments for jdbc connection (including the initial data filtering)
   */
  def createDataFrame(sc: SparkContext, arguments: JdbcArgs): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val (dbConnectionString, username, password) = JdbcFunctions.buildUrl(arguments.connectorType)

    val connect = new Properties()
    connect.setProperty("username", username)
    connect.setProperty("password", password)

    arguments.query match {
      case None => sqlContext.read.jdbc(dbConnectionString, arguments.tableName, connect)
      case Some(query) => sqlContext.read.jdbc(dbConnectionString, arguments.tableName, connect).sqlContext.sql(query)
    }
  }

  /**
   * Converts a jdbc data type to DataType
   * @param sparkDataType jdbc type
   * @return a DataType
   */
  def sparkDataTypeToSchemaDataType(sparkDataType: String): DataType = {
    FrameRdd.sparkDataTypeToSchemaDataType(sparkDataType)
  }
}
