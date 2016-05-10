package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import org.scalatest.WordSpec
import scala.collection.mutable.ArrayBuffer

class OrientDbTypeToDataTypeConverterTest extends WordSpec{

  "OrientDB type to data type converter" should {
    "Convert OType to DataType" in {
      val orientDbType = Array(OType.LONG,OType.STRING,OType.INTEGER)
      val dataTypeBuffer = new ArrayBuffer[String]()
      orientDbType.foreach(oType => {
        //call Method under test
        val dataType = OrientDbTypeConverter.convertOrientDbtoDataType(oType)
        dataTypeBuffer += dataType.toString
      })
      //validate results
      assert(dataTypeBuffer ==ArrayBuffer("int64","string","int32"))
    }
  }

}
