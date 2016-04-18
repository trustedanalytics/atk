package org.apache.spark.atk.graph

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wtaie on 4/6/16.
 */
class EdgeFrameRddTest extends WordSpec with Matchers with TestingSparkContextWordSpec {
  "EdgeFrameRdd" should {
    val columns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
    val schema = new EdgeSchema(columns, "label", "srclabel", "destlabel")
    val rows: List[Row] = List(
      new GenericRow(Array(1L, 1L, 2L, "distance1", 100)),
      new GenericRow(Array(2L, 1L, 3L, "distance2", 200)),
      new GenericRow(Array(3L, 3L, 4L, "distance3", 400)),
      new GenericRow(Array(4L, 2L, 3L, "distance4", 500)))
    "test map edges partitions " in {
      val rowRdd = sparkContext.parallelize(rows)
      val edgeFrameRdd = new EdgeFrameRdd(schema, rowRdd)
      val nameRdd = edgeFrameRdd.mapPartitionEdges(iter => {
        val nameBuffer = new ArrayBuffer[String]()

        while (iter.hasNext) {
          val edgeWrapper = iter.next() //Atk edge wrapper
          val nameProperty = edgeWrapper.stringValue("distance")
          nameBuffer += nameProperty
        }
        nameBuffer.toIterator
      })
      //colect the nameRdd into an array
      val nameList = nameRdd.collect()
      nameList should contain theSameElementsAs (Array("100", "200", "400", "500"))

    }
  }

}