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

package org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin

import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

import scala.collection.JavaConversions.asScalaIterator

/**
 * Split a string based on delimiter into List[String]
 * <p>
 * Usage:
 * scala> import com.trustedanalytics.engine.Row
 * scala> val out = RowParser.apply("foo,bar")
 * scala> val out = RowParser.apply("a,b,\"foo,is this ,bar\",foobar ")
 * scala> val out = RowParser.apply(" a,b,'',\"\"  ")
 * </p>
 *
 * @param separator delimiter character
 */
class CsvRowParser(separator: Char, columnTypes: Array[DataType]) extends Serializable {

  val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

  val converter = DataTypes.parseMany(columnTypes)(_)

  /**
   * Parse a line into a RowParseResult
   * @param line a single line
   * @return the result - either a success row or an error row
   */
  def apply(line: String): RowParseResult = {
    try {
      val parts = splitLineIntoParts(line)
      RowParseResult(parseSuccess = true, converter(parts.asInstanceOf[Array[Any]]))
    }
    catch {
      case e: Exception =>
        RowParseResult(parseSuccess = false, Array(line, e.toString))
    }
  }

  private[frame] def splitLineIntoParts(line: String): Array[String] = {
    val records = CSVParser.parse(line, csvFormat).getRecords
    if (records.isEmpty) {
      Array.empty[String]
    }
    else {
      records.get(0).iterator().toArray
    }

  }

}
