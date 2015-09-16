package org.trustedanalytics.atk.engine.model.plugins

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.{ LabeledPoint, LabeledPointWithFrequency }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.DataTypes

class FrameRddFunctions(self: FrameRdd) {
  /**
   * Convert FrameRdd into RDD[LabeledPoint] format required by MLLib
   */
  def toLabeledPointRDD(labelColumnName: String, featureColumnNames: List[String]): RDD[LabeledPoint] = {
    self.mapRows(row =>
      {
        val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
        new LabeledPoint(DataTypes.toDouble(row.value(labelColumnName)), new DenseVector(features.toArray))
      })
  }

  /**
   * Convert FrameRdd into RDD[LabeledPointWithFrequency] format required for updates in MLLib code
   */
  def toLabeledPointRDDWithFrequency(labelColumnName: String,
                                     featureColumnNames: List[String],
                                     frequencyColumnName: Option[String]): RDD[LabeledPointWithFrequency] = {
    self.mapRows(row =>
      {
        val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
        frequencyColumnName match {
          case Some(freqColumn) =>
            new LabeledPointWithFrequency(DataTypes.toDouble(row.value(labelColumnName)),
              new DenseVector(features.toArray), DataTypes.toDouble(row.value(freqColumn)))
          case _ =>
            new LabeledPointWithFrequency(DataTypes.toDouble(row.value(labelColumnName)),
              new DenseVector(features.toArray), DataTypes.toDouble(1.0))
        }
      })
  }

}
