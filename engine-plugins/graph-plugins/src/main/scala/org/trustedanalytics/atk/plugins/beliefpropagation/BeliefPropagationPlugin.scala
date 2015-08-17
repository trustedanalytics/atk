/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.plugins.beliefpropagation

import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.domain.DomainJsonProtocol

import spray.json._
/**
 * Variables for executing belief propagation.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 @ArgDoc("""Name of the vertex property which contains the prior belief
for the vertex.""") priorProperty: String,
                                 @ArgDoc("""Name of the vertex property which will contain the posterior
 belief for each vertex.""") posteriorProperty: String,
                                 @ArgDoc("""Name of the edge property that contains the edge weight for each edge.""") edgeWeightProperty: Option[String] = None,
                                 @ArgDoc("""Minimum average change in posterior beliefs between supersteps.
Belief propagation will terminate when the average change in posterior beliefs between supersteps is
less than or equal to this threshold.""") convergenceThreshold: Option[Double] = None,
                                 @ArgDoc("""The maximum number of supersteps that the algorithm will execute.
The valid range is all positive int.""") maxIterations: Option[Int] = None)

/**
 * Companion object holds the default values.
 */
object BeliefPropagationDefaults {
  val stringOutputDefault = false
  val maxIterationsDefault = 20
  val edgeWeightDefault = 1.0d
  val powerDefault = 0d
  val smoothingDefault = 2.0d
  val convergenceThreshold = 0d
}

/**
 * The result object
 *
 * @param frameDictionaryOutput dictionary with vertex label type as key and vertex's frame as the value
 * @param time execution time
 */
case class BeliefPropagationResult(frameDictionaryOutput: Map[String, FrameEntity], time: Double)

/** Json conversion for arguments and return value case classes */
object BeliefPropagationJsonFormat {
  import DomainJsonProtocol._
  implicit val BPFormat = jsonFormat6(BeliefPropagationArgs)
  implicit val BPResultFormat = jsonFormat2(BeliefPropagationResult)
}

import BeliefPropagationJsonFormat._

/**
 * Launches "loopy" belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
@PluginDoc(oneLine = "Classification on sparse data using belief propagation.",
  extended = """Belief propagation by the sum-product algorithm.
This algorithm analyzes a graphical model with prior beliefs using sum product message passing.
The priors are read from a property in the graph, the posteriors are written to another property in the graph.
This is the GraphX-based implementation of belief propagation.

|
**Label Propagation (LP)**

Label propagation (LP) is a message passing technique for inputing or
:term:`smoothing` labels in partially-labelled datasets.
Labels are propagated from *labeled* data to *unlabeled* data along a graph
encoding similarity relationships among data points.
The labels of known data can be probabilistic, in other words, a known point
can be represented with fuzzy labels such as 90% label 0 and 10% label 1.
The inverse distance between data points is represented by edge weights, with
closer points having a higher weight (stronger influence
on posterior estimates) than points farther away.
LP has been used for many problems, particularly those involving a similarity
measure between data points.
Our implementation is based on Zhu and Ghahramani's 2002 paper,
`Learning from labeled and unlabeled data. <http://www.cs.cmu.edu/~zhuxj/pub/CMU-CALD-02-107.pdf>`__.

**The Label Propagation Algorithm**

In LP, all nodes start with a prior distribution of states and the initial
messages vertices pass to their neighbors are simply their prior beliefs.
If certain observations have states that are known deterministically, they can
be given a prior probability of 100% for their true state and 0% for all others.
Unknown observations should be given uninformative priors.

Each node, :math:`i`, receives messages from its :math:`k` neighbors and
updates its beliefs by taking a weighted average of its current beliefs
and a weighted average of the messages received from its neighbors.

The updated beliefs for node :math:`i` are:

.. math::

    updated\ beliefs_{i} = \lambda * (prior\ belief_{i} ) + (1 - \lambda ) \
    * \sum_k w_{i,k} * previous\ belief_{k}

where :math:`w_{i,k}` is the normalized weight between nodes :math:`i` and
:math:`k`, normalized such that the sum of all weights to neighbors is 1.

:math:`\lambda` is a leaning parameter.
If :math:`\lambda` is greater than zero, updated probabilities will be anchored
in the direction of prior beliefs.

The final distribution of state probabilities will also tend to be biased in
the direction of the distribution of initial beliefs.
For the first iteration of updates, nodes' previous beliefs are equal to the
priors, and, in each future iteration,
previous beliefs are equal to their beliefs as of the last iteration.
All beliefs for every node will be updated in this fashion, including known
observations, unless ``anchor_threshold`` is set.
The ``anchor_threshold`` parameter specifies a probability threshold above
which beliefs should no longer be updated.
Hence, with an ``anchor_threshold`` of 0.99, observations with states known
with 100% certainty will not be updated by this algorithm.

This process of updating and message passing continues until the convergence
criteria is met, or the maximum number of :term:`supersteps` is reached.
A node is said to converge if the total change in its cost function is below
the convergence threshold.
The cost function for a node is given by:

.. math::

    cost =& \sum_k w_{i,k} * \Big[ \big( 1 - \lambda \big) * \big[ previous\ \
    belief_{i}^{2} - w_{i,k} * previous\ belief_{i} * \\
    & previous\ belief_{k} \big] + 0.5 * \lambda * \big( previous\ belief_{i} \
    - prior_{i} \big) ^{2} \Big]


Convergence is a local phenomenon; not all nodes will converge at the same time.
It is also possible that some (most) nodes will converge and others will not converge.
The algorithm requires all nodes to converge before declaring global convergence.
If this condition is not met, the algorithm will continue up to the maximum
number of :term:`supersteps`.
""",
  returns = "Progress report for belief propagation in the format of a multiple-line string.")
class BeliefPropagationPlugin extends SparkCommandPlugin[BeliefPropagationArgs, BeliefPropagationResult] {

  override def name: String = "graph/ml/belief_propagation"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: BeliefPropagationArgs)(implicit invocation: Invocation): Int = {
    // TODO: not sure this is right but it seemed to work with testing
    //    when max iterations was 1, number of jobs was 11
    //    when max iterations was 2, number of jobs was 13
    //    when max iterations was 3, number of jobs was 15
    //    when max iterations was 4, number of jobs was 17
    //    when max iterations was 5, number of jobs was 19
    //    when max iterations was 6, number of jobs was 21
    9 + (arguments.maxIterations.getOrElse(0) * 2)
  }

  override def execute(arguments: BeliefPropagationArgs)(implicit invocation: Invocation): BeliefPropagationResult = {

    val start = System.currentTimeMillis()

    // Get the graph
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val bpRunnerArgs = BeliefPropagationRunnerArgs(arguments.posteriorProperty,
      arguments.priorProperty,
      arguments.maxIterations,
      stringOutput = Some(true), // string output is default until the ATK supports Vectors as a datatype in tables
      arguments.convergenceThreshold,
      arguments.edgeWeightProperty)

    val (outVertices, outEdges, log) = BeliefPropagationRunner.run(gbVertices, gbEdges, bpRunnerArgs)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)
    val frameMap = frameRddMap.keys.map(label => {
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    BeliefPropagationResult(frameMap, time)

  }
}
