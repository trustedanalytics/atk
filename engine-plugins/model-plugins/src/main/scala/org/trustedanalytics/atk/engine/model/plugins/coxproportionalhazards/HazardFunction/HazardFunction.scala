package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.CoxProportionalHazardTrainFunctions

/**
 * Predicts an event using the hazard function of the Cox proportional model
 */
class HazardFunction {

  /**
   * Predicts an event
   * @param rdd initial rdd
   * @param beta (fitted) beta
   * @param timeCol the time column in the initial rdd
   * @param covariateCol the covariate column in the initial rdd
   * @param censoredCol the censored column in the initial rdd
   * @return a predicted hazard
   */
  def predict(rdd: FrameRdd, beta: Double, timeCol: String, covariateCol: String, censoredCol: String): Double = {
    val sortedRdd = CoxProportionalHazardTrainFunctions.frameToSortedTupleRdd(rdd, timeCol, covariateCol, censoredCol)
    val rddWithExpColumns = HazardFunctionBetaEstimator.hazardFunctionRdd(sortedRdd, beta)

    predict(rddWithExpColumns, beta)
  }

  /**
   * Predicts an event using a fitted beta (already part of the exponential columns of the input rdd)
   * @param rdd the input frame
   * @return a predicted hazard
   */
  private def predict(rdd: RDD[(HazardFunctionRow, Long)], beta: Double): Double =
    {
      //TODO: Consider a parallel implementation and replace the loop below
      val hazardFuncRdd = rdd.map { case (row, index) => row }
      val initialValueRdd = hazardFuncRdd.map(row => row.x)
      val colSum = CoxProportionalHazardTrainFunctions.columnSum(initialValueRdd)

      val maxIndex = rdd.count - 1
      var exp = 0.0
      var sum = 0.0

      for (index <- maxIndex to 0 by -1) {
        val row = HazardFunctionBetaEstimator.getRowAtIndex(rdd, index)

        exp += row.exp
        sum += Math.log(exp)
      }

      beta * colSum - sum
    }

}
