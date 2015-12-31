/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.engine.frame.plugins.classificationmetrics

import org.trustedanalytics.atk.domain.SerializableType
import org.trustedanalytics.atk.domain.frame.{ ClassificationMetricArgs, ClassificationMetricValue }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.frame.plugins.ClassificationMetrics
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Computes Model accuracy, precision, recall, confusion matrix and f_measure
 *
 */
@PluginDoc(oneLine = "Model statistics of accuracy, precision, and others.",
  extended = """Calculate the accuracy, precision, confusion_matrix, recall and
:math:`F_{ \beta}` measure for a classification model.

*   The **f_measure** result is the :math:`F_{ \beta}` measure for a
    classification model.
    The :math:`F_{ \beta}` measure of a binary classification model is the
    harmonic mean of precision and recall.
    If we let:

    * beta :math:`\equiv \beta`,
    * :math:`T_{P}` denotes the number of true positives,
    * :math:`F_{P}` denotes the number of false positives, and
    * :math:`F_{N}` denotes the number of false negatives

    then:

    .. math::

        F_{ \beta} = (1 + \beta ^ 2) * \frac{ \frac{T_{P}}{T_{P} + F_{P}} * \
        \frac{T_{P}}{T_{P} + F_{N}}}{ \beta ^ 2 * \frac{T_{P}}{T_{P} + \
        F_{P}}  + \frac{T_{P}}{T_{P} + F_{N}}}

    The :math:`F_{ \beta}` measure for a multi-class classification model is
    computed as the weighted average of the :math:`F_{ \beta}` measure for
    each label, where the weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **recall** result of a binary classification model is the proportion
    of positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{N}` denote the number of false negatives, then the model
    recall is given by :math:`\frac {T_{P}} {T_{P} + F_{N}}`.

    For multi-class classification models, the recall measure is computed as
    the weighted average of the recall for each label, where the weight is
    the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **precision** of a binary classification model is the proportion of
    predicted positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{P}` denote the number of false positives, then the model
    precision is given by: :math:`\frac {T_{P}} {T_{P} + F_{P}}`.

    For multi-class classification models, the precision measure is computed
    as the weighted average of the precision for each label, where the
    weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **accuracy** of a classification model is the proportion of
    predictions that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives,
    :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
    the total number of classified instances, then the model accuracy is
    given by: :math:`\frac{T_{P} + T_{N}}{K}`.

    This measure applies to binary and multi-class classifiers.

*   The **confusion_matrix** result is a confusion matrix for a
    binary classifier model, formatted for human readability.

Notes
-----
The **confusion_matrix** is not yet implemented for multi-class classifiers.""",
  returns = """The data returned is composed of multiple components\:

|   <object>.accuracy : double
|   <object>.confusion_matrix : table
|   <object>.f_measure : double
|   <object>.precision : double
|   <object>.recall : double""")
class ClassificationMetricsPlugin extends SparkCommandPlugin[ClassificationMetricArgs, ClassificationMetricValue] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/classification_metrics"
  /**
   * Set the kryo class to use
   */
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: ClassificationMetricArgs)(implicit invocation: Invocation) = 8
  /**
   * Computes Model accuracy, precision, recall, confusion matrix and f_measure
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationMetricArgs)(implicit invocation: Invocation): ClassificationMetricValue = {
    val frame: SparkFrame = arguments.frame

    // check if poslabel is an Int, string or None
    import org.trustedanalytics.atk.domain.SerializableType._

    val betaValue = arguments.beta.getOrElse(1.0)
    val classificationMetrics = arguments.posLabel match {
      case Some(Left(stringPositiveLabel)) =>
        ClassificationMetrics.binaryClassificationMetrics(
          frame.rdd,
          arguments.labelColumn,
          arguments.predColumn,
          stringPositiveLabel,
          betaValue,
          arguments.frequencyColumn
        )
      case Some(Right(intPositiveLabel)) => {
        ClassificationMetrics.binaryClassificationMetrics(
          frame.rdd,
          arguments.labelColumn,
          arguments.predColumn,
          intPositiveLabel,
          betaValue,
          arguments.frequencyColumn
        )
      }
      case _ => {
        ClassificationMetrics.multiclassClassificationMetrics(
          frame.rdd,
          arguments.labelColumn,
          arguments.predColumn,
          betaValue,
          arguments.frequencyColumn
        )
      }
    }
    classificationMetrics
  }
}
