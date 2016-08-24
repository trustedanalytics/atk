package org.trustedanalytics.atk.scoring.models

import org.apache.spark.mllib.regression.LassoModel

/**
 * Command for loading model data into existing model in the model database.
 * @param lassoModel Trained MLLib's LassoModel object
 * @param observationColumns Handle to the observation columns of the data frame
 */
case class LassoData(lassoModel: LassoModel, observationColumns: List[String]) {
  require(lassoModel != null, "lassoModel must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
}
