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

import com.orientechnologies.orient.core.metadata.schema.OType
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 * an object to convert DataTypes to OrientDb data type
 */
object OrientDbTypeConverter {
  /**
   * Method for converting data types to Orient data types
   *
   * @param dataType ATK data types
   * @return OrientDB data type
   */
  def convertDataTypeToOrientDbType(dataType: DataType): OType = dataType match {
    case int64 if dataType.equalsDataType(DataTypes.int64) => OType.LONG
    case int32 if dataType.equalsDataType(DataTypes.int32) => OType.INTEGER
    case float32 if dataType.equalsDataType(DataTypes.float32) => OType.FLOAT
    case float64 if dataType.equalsDataType(DataTypes.float64) => OType.FLOAT
    case string if dataType.equalsDataType(DataTypes.string) => OType.STRING
    case _ => throw new IllegalArgumentException(s"Unable to convert $dataType to OrientDb data type")
  }

}
