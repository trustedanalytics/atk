/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin

/**
 * Parsing results into two types of rows: successes and failures.
 *
 * Successes go into one data frame and errors go into another.
 *
 * @param parseSuccess true if this row was a success, false otherwise
 * @param row either the successfully parsed row OR a row of two columns (original line and error message)
 */
case class RowParseResult(parseSuccess: Boolean, row: Array[Any]) {
  if (!parseSuccess) {
    require(row.length == 2, "error rows have two columns: the original un-parsed line and the error message")
  }
}
