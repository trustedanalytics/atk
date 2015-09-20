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

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
/**
 * Create a 'new' instance of a NaiveBayes model
 */

@PluginDoc(oneLine = "Create a 'new' instance of a Naive Bayes model",
  extended = """
**Classification using Naive Bayes**

Naive Bayes[1]_ is a probabilistic classifier with strong independence assumptions between features. It computes the conditional
probability distribution of each feature given label, and then applies Bayes' theorem to compute the conditional probability distribution
of label given an observation and use it for prediction. The user may initialize a NaiveBayesModel, train the model on columns of a frame
and use the trained model to predict the value of the dependent variable given the independent observations of a frame. This model
runs the MLLib implementation of Naive Bayes[2]_.

.. [1] https://en.wikipedia.org/wiki/Naive_Bayes_classifier
.. [2] https://spark.apache.org/docs/1.3.0/mllib-naive-bayes.html
             """)
class NaiveBayesNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:naive_bayes")))
  }
}

