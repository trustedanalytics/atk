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

import org.apache.spark._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.rdd.{ RDD }
import org.trustedanalytics.atk.domain.frame.load.{ JdbcArgs }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 * Helper class for creating an RDD from hBase
 */
object LoadJdbcImpl extends Serializable {

  private val user = "root"
  private val password = "root"

  /**
   * Create a data frame from an jdbc compatible database
   * @param sc default spark context
   * @param arguments arguments for jdbc connection (including the initial data filtering)
   */
  def createDataFrame(sc: SparkContext, arguments: JdbcArgs): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val options: Map[String, String] = Map(
      "driver" -> arguments.driverName,
      "url" -> JdbcConnectionFactory.connectionUrl(arguments),
      "dbtable" -> arguments.initialQuery.getOrElse("select * from " + arguments.databaseName)
    )

    sqlContext.load("jdbc", options)
  }

  /**
   * Converts a jdbc data type to DataType
   * @param dbType jdbc type
   * @return a DataType
   */
  def dbToDataType(dbType: String): DataType = {
    if ("int".equalsIgnoreCase(dbType)) {
      DataTypes.int32
    }
    else if ("long".equalsIgnoreCase(dbType)) {
      DataTypes.int32
    }
    else if ("double".equalsIgnoreCase(dbType)) {
      DataTypes.float64
    }
    else if ("string".equalsIgnoreCase(dbType)) {
      DataTypes.string
    }
    else throw new IllegalArgumentException(s"unsupported export type $dbType")
  }
}
