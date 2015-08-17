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

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.apache.commons.csv.{ CSVFormat, CSVParser }

import scala.collection.JavaConversions.asScalaIterator

class UploadParser(columnTypes: Array[DataType]) extends Serializable {

  val converter = DataTypes.parseMany(columnTypes)(_)

  /**
   * Parse a line into a RowParseResult
   * @param row a single line
   * @return the result - either a success row or an error row
   */
  def apply(row: List[Any]): RowParseResult = {
    try {
      RowParseResult(parseSuccess = true, converter(row.toArray))
    }
    catch {
      case e: Exception =>
        RowParseResult(parseSuccess = false, Array(row.mkString(","), e.toString))
    }
  }
}
