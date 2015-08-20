//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol._

/**
 * Command for loading model data into existing model in the model database.
 * @param randomForestModel Trained MLLib's LinearRegressionModel object
 * @param observationColumns Handle to the observation columns of the data frame
 * @param numClasses Number of classes of the data
 */
case class RandomForestClassifierData(randomForestModel: RandomForestModel, observationColumns: List[String], numClasses: Int) {
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumns must not be null nor empty")
  require(randomForestModel != null, "randomForestModel must not be null")
}
