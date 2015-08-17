package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.mllib.regression.LinearRegressionModel

/**
 * Command for loading model data into existing model in the model database.
 * @param linRegModel Trained MLLib's LinearRegressionModel object
 * @param observationColumns Handle to the observation columns of the data frame
 */
case class LinearRegressionData(linRegModel: LinearRegressionModel, observationColumns: List[String]) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(linRegModel != null, "linRegModel must not be null")
}
