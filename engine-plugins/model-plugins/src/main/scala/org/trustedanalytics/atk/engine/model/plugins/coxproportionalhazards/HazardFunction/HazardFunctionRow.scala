package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction

/**
 * Hazard function row, containing the exponent values pre-calculated
 * @param time time variable
 * @param x value parameter
 * @param exp exp(x)
 * @param xTimesExp x * exp(x)
 * @param xSquaredTimesExp pow (x,2) * exp(x)
 */
case class HazardFunctionRow(time: Double,
                             x: Double,
                             exp: Double,
                             xTimesExp: Double,
                             xSquaredTimesExp: Double) {

}

/**
 * Pair of feature column and their censored values
 * @param x
 * @param censored
 */
case class CensoredFeaturePair(x: Double, censored: Int) {

}