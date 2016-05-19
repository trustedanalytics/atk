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
import org.scalatest.WordSpec
import scala.collection.mutable.ArrayBuffer

class OrientDbTypeToDataTypeConverterTest extends WordSpec {

  "OrientDB type to data type converter" should {
    "Convert OType to DataType" in {
      val orientDbType = Array(OType.LONG, OType.STRING, OType.INTEGER)
      val dataTypeBuffer = new ArrayBuffer[String]()
      orientDbType.foreach(oType => {
        //call Method under test
        val dataType = OrientDbTypeConverter.convertOrientDbtoDataType(oType)
        dataTypeBuffer += dataType.toString
      })
      //validate results
      assert(dataTypeBuffer == ArrayBuffer("int64", "string", "int32"))
    }
  }

}
