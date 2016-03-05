package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object CoxProportionalHazardJSonFormat {

  implicit val coxProportionalHazardTrainFormat = jsonFormat8(CoxProportionalHazardTrainArgs)
  implicit val coxProportionalHazardPredictFormat = jsonFormat1(CoxProportionalHazardsPredictArgs)
  implicit val coxProportionalHazardTrainReturn = jsonFormat2(CoxProportionalHazardTrainReturn)
}
