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
package org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame

import org.apache.spark.sql.catalyst.{ CatalystTypeConverters, InternalRow }
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

object RowUtils extends Serializable {

  /**
   * Convert array to Catalyst internal row format
   *
   * @param array Input array
   * @return Catalyst row
   */
  def convertToCatalystRow(array: Array[Any]): InternalRow = {
    val catalystArr = array.map(x => CatalystTypeConverters.convertToCatalyst(x))
    new GenericMutableRow(catalystArr)
  }
}
