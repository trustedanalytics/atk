package org.apache.spark.mllib.regression

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.util.NumericParser

import scala.beans.BeanInfo

/**
 * Class that represents the features and labels of a data point.
 *
 * Extension of MlLib's labeled points that supports a frequency column.
 * The frequency column contains the frequency of occurrence of each observation.
 * @see org.apache.spark.mllib.regression.LabeledPoint
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@BeanInfo
case class LabeledPointWithFrequency(label: Double, features: Vector, frequency: Double) {
  override def toString: String = {
    s"($label,$features,$frequency)"
  }
}

/**
 * Parser for [[org.apache.spark.mllib.regression.LabeledPointWithFrequency]].
 */
object LabeledPointWithFrequency {
  /**
   * Parses a string resulted from `LabeledPointWithFrequency#toString` into
   * an [[org.apache.spark.mllib.regression.LabeledPointWithFrequency]].
   */
  def parse(s: String): LabeledPointWithFrequency = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any, frequency: Double) =>
          LabeledPointWithFrequency(label, Vectors.parseNumeric(numeric), frequency)
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    }
    else { // dense format used before v1.0
      val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      val frequency = java.lang.Double.parseDouble(parts(2))
      LabeledPointWithFrequency(label, features, frequency)
    }
  }
}
