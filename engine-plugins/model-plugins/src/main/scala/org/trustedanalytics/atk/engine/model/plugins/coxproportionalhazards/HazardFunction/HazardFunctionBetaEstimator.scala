package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction

import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.CoxProportionalHazardTrainFunctions

/**
 * Calculates the cox proportional hazard function beta using a Newton-Raphson algorithm
 */
object HazardFunctionBetaEstimator {

  /**
   * Calculates beta for the hazard function using Newton-Raphson algorithm
   * @param sortedRdd initial rdd (sorted ascending for time)
   * @param convergenceEps convergence epsilon
   * @return the final beta for hazard function
   */
  def newtonRaphson(sortedRdd: RDD[(Double, Double)], convergenceEps: Double, maxSteps: Int, initialBeta: Double): (Double, Double) = {

    var currentStepBeta = initialBeta
    var error = 0.0
    var currentStep = 1

    do {
      val sortedRddWithExpColumns = hazardFunctionRdd(sortedRdd, currentStepBeta)
      val beta = nextBeta(sortedRddWithExpColumns, currentStepBeta)

      error = Math.abs(beta - currentStepBeta)
      currentStepBeta = beta
      currentStep += 1
    } while ((error > convergenceEps) && (currentStep < maxSteps))

    (currentStepBeta, error)
  }

  /**
   * Calculates the first derivative for a particular beta (already part of the exponential columns of the input rdd)
   * @param rdd the input frame
   * @return the first derivative
   */
  def firstAndSecondDerivative(rdd: RDD[(HazardFunctionRow, Long)]): (Double, Double) =
    {
      val hazardFuncRdd = rdd.map { case (value, index) => value }
      val initialValueRdd = hazardFuncRdd.map(row => row.x)
      val colSum = CoxProportionalHazardTrainFunctions.columnSum(initialValueRdd)

      val maxIndex = rdd.count - 1
      var exp = 0.0
      var xTimesExp = 0.0
      var xSquaredTimesExp = 0.0
      var firstDerivative = 0.0
      var secondDerivative = 0.0

      for (index <- maxIndex to 0 by -1) {
        val row = getRowAtIndex(rdd, index)

        exp += row.exp
        xTimesExp += row.xTimesExp
        xSquaredTimesExp += row.xSquaredTimesExp

        //FIRST derivative
        firstDerivative += (xTimesExp / exp)

        //SECOND derivative
        val secondDerivativeNumerator = xSquaredTimesExp * (exp - xTimesExp)
        val secondDerivativeDenominator = Math.pow(exp, 2)
        secondDerivative += (secondDerivativeNumerator / secondDerivativeDenominator)
      }

      (colSum - firstDerivative, 0.0 - secondDerivative)
    }

  /**
   * Calculates the next step beta (newton-Rapson is an iterative algorithm)
   * @param hazardFuncRdd the input frame
   * @param beta the current step beta
   * @return the next step beta
   */
  def nextBeta(hazardFuncRdd: RDD[(HazardFunctionRow, Long)], beta: Double): Double = {

    val (first, second) = firstAndSecondDerivative(hazardFuncRdd)

    beta + (first / second)
  }

  /**
   * Add exponential functions as rdd columns
   * @param sortedRdd initial rdd (time, covariance)
   * @param expMultiplier beta multiplier for exponent
   * @return rdd in the format (time, exp(x), x*exp(x), (x*x)exp(x))
   */
  def hazardFunctionRdd(sortedRdd: RDD[(Double, Double)], expMultiplier: Double): RDD[(HazardFunctionRow, Long)] = {
    val hazardFuncRdd = sortedRdd.map { row =>
      val time = row._1
      val x = row._2

      val exp = Math.exp(x * expMultiplier)
      val xTimesExp = x * exp
      val xSquaredTimesExp = x * xTimesExp

      HazardFunctionRow(time, x, exp, xTimesExp, xSquaredTimesExp)
    }

    hazardFuncRdd.zipWithIndex()
  }

  /**
   * Returns the row at index
   * @param rdd rdd with exponential values, zipped with index
   * @param index an index
   * @return the row at index
   */
  def getRowAtIndex(rdd: RDD[(HazardFunctionRow, Long)], index: Long): HazardFunctionRow = {

    rdd.filter(_._2 == index).map(_._1).first()
  }
}
