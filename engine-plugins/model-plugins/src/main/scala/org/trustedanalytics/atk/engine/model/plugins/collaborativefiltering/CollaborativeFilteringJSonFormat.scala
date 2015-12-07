package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object CollaborativeFilteringJsonFormat {

  implicit val cfTrainArgs = jsonFormat13(CollaborativeFilteringTrainArgs)
  implicit val cfScoreArgs = jsonFormat3(CollaborativeFilteringScoreArgs)
  implicit val cfPredictArgs = jsonFormat7(CollaborativeFilteringPredictArgs)
  implicit val cfRecommendArgs = jsonFormat4(CollaborativeFilteringRecommendArgs)
  implicit val cfRecommendSingleReturn = jsonFormat3(CollaborativeFilteringSingleRecommendReturn)
  implicit val cfRecommendReturn = jsonFormat1(CollaborativeFilteringRecommendReturn)
  implicit val cfFilterArgs = jsonFormat3(CollaborativeFilteringData)
}
