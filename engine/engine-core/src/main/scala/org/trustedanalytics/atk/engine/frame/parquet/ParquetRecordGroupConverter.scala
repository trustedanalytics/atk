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

package org.trustedanalytics.atk.engine.frame.parquet

import parquet.io.api.{ Converter, GroupConverter }

/**
 *  Class used by the Parquet libraries to convert a group of objects into a group of primitive objects
 */
class ParquetRecordGroupConverter extends GroupConverter {
  /** called at the beginning of the group managed by this converter */
  override def start(): Unit = {}
  /**
   * call at the end of the group
   */
  override def end(): Unit = {}

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  override def getConverter(fieldIndex: Int): Converter = {
    new ParquetRecordConverter()
  }
}
