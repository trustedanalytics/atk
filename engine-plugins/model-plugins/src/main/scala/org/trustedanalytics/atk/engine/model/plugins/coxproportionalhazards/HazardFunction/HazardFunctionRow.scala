package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction

/**
 * Hazard function row, containing the exponent values pre-calculated
 * @param time time variable
 * @param x covariate parameter
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