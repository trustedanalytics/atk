package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ DataTypes }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Helper class for collaborative filtering*/
object CollaborativeFilteringHelper {
  def toAlsRdd(userFrame: FrameRdd, data: CollaborativeFilteringData): RDD[(Int, Array[Double])] = {
    val idColName = data.userFrame.schema.column(0).name
    val featuresColName = data.userFrame.schema.column(1).name

    userFrame.mapRows(row => (row.intValue(idColName),
      DataTypes.vector.parse(row.value(featuresColName)).get.toArray))
  }

}
