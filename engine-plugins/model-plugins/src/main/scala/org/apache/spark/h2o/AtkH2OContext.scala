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
package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object AtkH2OContext extends org.apache.spark.Logging with Serializable {

  /**
   * Get or create H2O context
   *
   * This method works around an issue with stale references to the Spark
   * context in the H2O context during subsequent ATK calls. The H2O context
   * is a singleton object which cannot be initialized multiple times.
   *
   * @param sc Spark Context
   */
  def init(sc: SparkContext): H2OContext = synchronized {
    val h2oContext = H2OContext.getOrCreate(sc)
    val sqlContext = SQLContext.getOrCreate(sc)

    //Re-set spark context reference in H2O context to prevent stale references
    //Stop-gap solution till we migrate to spark-tk where sparkling water is available as a library
    val sparkContextField = classOf[H2OContext].getDeclaredField("sparkContext")
    val h2oConfField = classOf[H2OContext].getDeclaredField("_conf")
    val sqlContextField = classOf[H2OContext].getDeclaredField("sqlc")

    //Change accessibility of private fields
    sparkContextField.setAccessible(true)
    h2oConfField.setAccessible(true)
    sqlContextField.setAccessible(true)

    //Set spark-related variables
    sparkContextField.set(h2oContext, sc)
    sqlContextField.set(h2oContext, sqlContext)

    h2oContext
  }
}
