package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** JSON conversion for arguments and return value case classes */
object DaalPrincipalComponentsJsonFormat {
  implicit val daalSvdDataFormat = jsonFormat6(DaalSvdData)
  implicit val daalPcaTrainArgsFormat = jsonFormat5(DaalPrincipalComponentsTrainArgs)
  implicit val daalPcaTrainReturnFormat = jsonFormat6(DaalPrincipalComponentsTrainReturn)
}

