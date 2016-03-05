package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards

import org.apache.commons.lang3.StringUtils
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.plugins.cumulativedist.CumulativeDistFunctions
import org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction.{ CensoredFeaturePair, HazardFunctionRow }

/**
 * Helper class for Cox proportional hazard model
 */
object CoxProportionalHazardTrainFunctions {

  /**
   * Sort the initial frame on the time column
   * @param rdd the initial RDD
   * @param timeCol index of the time column
   * @param covariateCol index of the covariate column
   * @return sorted RDD
   */
  def frameToSortedTupleRdd(rdd: FrameRdd, timeCol: String, covariateCol: String, censoredCol: String): RDD[(Double, Double)] = {
    val rowWrapper = rdd.rowWrapper
    val hasCensoredColumn = StringUtils.isNotBlank(censoredCol)

    val filteredRdd = if (hasCensoredColumn) rdd.filter(row => rowWrapper(row).intValue(censoredCol) == 1) else rdd
    val tupleRdd = try {
      filteredRdd.map { row =>
        val timeValue = rowWrapper(row).doubleValue(timeCol)
        val covariateValue = rowWrapper(row).doubleValue(covariateCol)

        (timeValue, covariateValue)
      }
    }
    catch {
      case exception: NumberFormatException => throw
        new NumberFormatException("Column values need to be numeric: " + exception.toString)
    }

    if (tupleRdd.count() == 0) {
      throw new RuntimeException("All events censored, nothing to calculate")
    }
    else {
      tupleRdd.sortByKey(ascending = true)
    }
  }

  /**
   * Converts a paired rdd[(double, double)] to frameRdd
   * @param rdd paired rdd[(double, double)]
   * @return a frameRdd
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
        val rowArray = new Array[Any](7)

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

    CumulativeDistFunctions.partitionSums(rdd).sum
  }

}
