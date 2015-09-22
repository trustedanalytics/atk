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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation, SparkCommandPlugin }
import org.trustedanalytics.atk.engine.PluginDocAnnotation
<<<<<<< HEAD
=======
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation, SparkCommandPlugin }
>>>>>>> upstream/master
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Create a 'new' instance of a Random Forest Regressor model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Random Forest Regressor model.",
  extended = """
**Regression using Random Forest**

Random Forest [1]_ is a supervised ensemble learning algorithm used to perform
regression.
A Random Forest Regressor model is initialized, trained on columns of a frame,
and used to predict the value of each observation in the frame.
This model runs the MLLib implementation of Random Forest [2]_.
During training, the decision trees are trained in parallel.
During prediction, the average over-all tree's predicted value is the predicted
value of the random forest.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Random_forest
.. [2] https://spark.apache.org/docs/1.3.0/mllib-ensembles.html
             """)
class RandomForestRegressorNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_regressor/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:random_forest_regressor")))
  }
}

