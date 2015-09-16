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

package org.trustedanalytics.atk.graphbuilder.parser

/**
 * Parser parses objects of type T from rows using the supplied InputSchema.
 */
abstract class Parser[T](inputSchema: InputSchema) extends Serializable {

  /**
   * Parse a row of data into zero to many T
   *
   * Random access is needed so preferably an IndexedSeq[String] is supplied
   */
  def parse(row: Seq[Any]): Seq[T] = {
    parse(new InputRow(inputSchema, row))
  }

  /**
   * Parse a row of data into zero to many T
   */
  def parse(row: InputRow): Seq[T]
}
