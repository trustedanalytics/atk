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
package org.trustedanalytics.atk.engine.frame.parquet

import parquet.io.api.{ GroupConverter, PrimitiveConverter }

/**
 * Class used by Parquet libraries for converting binary information into a primitive type.
 */
class ParquetRecordConverter extends PrimitiveConverter {
  /**
   * For our purposes this will always be a primitive type
   */
  override def isPrimitive: Boolean = {
    true
  }

  /**
   * Creates a new GroupConverter object that is needed by the Parquet libraries
   */
  override def asGroupConverter(): GroupConverter = {
    new ParquetRecordGroupConverter()
  }
}
