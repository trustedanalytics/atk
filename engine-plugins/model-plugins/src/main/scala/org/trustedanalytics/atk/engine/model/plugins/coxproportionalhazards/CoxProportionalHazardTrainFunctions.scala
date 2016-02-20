package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.plugins.cumulativedist.CumulativeDistFunctions
import org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction.HazardFunctionRow

/**
 * Helper class for Cox proportional hazard model
 */
object CoxProportionalHazardTrainFunctions {

  /**
   * Sort the initial frame on the time column
   * @param rdd the initial RDD
   * @param timeColIndex index of the time column
   * @param covarianceColIndex index of the covariance column
   * @return sorted RDD
   */
  def frameToSortedPairRdd(rdd: FrameRdd, timeColIndex: Int, covarianceColIndex: Int): RDD[(Double, Double)] = {
    val pairedRdd = try {
      rdd.map { row =>
        val timeValue = java.lang.Double.parseDouble(row(timeColIndex).toString)
        val covarianceValue = java.lang.Double.parseDouble(row(covarianceColIndex).toString)

        (timeValue, covarianceValue)
      }
    }
    catch {
      case exception: NumberFormatException => throw
        new NumberFormatException("Column values need to be numeric: " + exception.toString)
    }

    pairedRdd.sortByKey(ascending = true)
  }

  /**
   * Converts a paired rdd[(double, double)] to frameRdd
   * @param rdd paired rdd[(double, double)]
   * @return frameRdd
   */
  def pairToFrameRdd(rdd: RDD[(Double, Double)]): RDD[Row] = {

    rdd.map {
      case (time, value) => {
        val rowArray = new Array[Any](2)
        rowArray(0) = time
        rowArray(1) = value
        new GenericRow(rowArray)
      }
    }
  }

  /**
   * Converts a HazardFunctionRdd to frameRdd
   * @param rdd HazardFunctionRdd
   * @return frameRdd
   */
  def hazardFuncToFrameRdd(rdd: RDD[(HazardFunctionRow, Long)]): RDD[Row] = {
    rdd.map {
      case (hazardRow, index) => {
        val rowArray = new Array[Any](6)

        rowArray(0) = hazardRow.time
        rowArray(1) = hazardRow.x
        rowArray(2) = hazardRow.exp
        rowArray(3) = hazardRow.xTimesExp
        rowArray(4) = hazardRow.xSquaredTimesExp

        //not part of the hazard function values but added to the row for completion
        rowArray(5) = DataTypes.toDouble(index)

        new GenericRow(rowArray)
      }
    }
  }

  /**
   * Calculates the sum of a numeric column
   * @param rdd input frame
   * @return the sum as double
   */
  def columnSum(rdd: RDD[Double]): Double = {

    CumulativeDistFunctions.columnSum(rdd)
  }

}
