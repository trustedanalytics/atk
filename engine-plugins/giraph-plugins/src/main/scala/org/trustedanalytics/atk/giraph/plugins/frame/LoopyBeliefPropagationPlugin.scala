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

package org.trustedanalytics.atk.giraph.plugins.frame

import org.apache.spark.sql.parquet.atk.giraph.frame.lbp.{ LoopyBeliefPropagationVertexOutputFormat, LoopyBeliefPropagationVertexInputFormat, LoopyBeliefPropagationEdgeInputFormat }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import org.trustedanalytics.atk.giraph.algorithms.lbp.LoopyBeliefPropagationComputation.{ LoopyBeliefPropagationAggregatorWriter, LoopyBeliefPropagationMasterCompute }
import org.trustedanalytics.atk.giraph.config.lbp._
import org.trustedanalytics.atk.giraph.plugins.util.{ GiraphConfigurationUtil, GiraphJobManager }
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, CommandPlugin, Invocation }

import LoopyBeliefPropagationJsonFormat._

@PluginDoc(oneLine = "Loopy Belief Propagation (LBP) from http://en.wikipedia.org/wiki/Belief_propagation",
  extended = """Loopy belief propagation on :term:`Markov Random Fields` (MRF).
This algorithm was originally designed for acyclic graphical models, then it
was found that the Belief Propagation algorithm can be used in general graphs.
The algorithm is then sometimes called "loopy" belief propagation,
because graphs typically contain cycles, or loops.

In Giraph, the algorithm runs in iterations until it converges.

|
**Loopy Belief Propagation (LBP)**

See: http://en.wikipedia.org/wiki/Belief_propagation.

Loopy Belief Propagation (LBP) is a message passing algorithm for inferring
state probabilities, given a graph and a set of noisy initial estimates of
state probabilities.
The |PACKAGE| provides two implementations of LBP, which differ in their
assumptions about the joint distribution of the data.
The standard LBP implementation assumes that the joint distribution of the
data is given by a Boltzmann distribution, while Gaussian LBP
assumes that the data is continuous and distributed according to a multivariate
normal distribution.
For more information about LBP, see: "K. Murphy, Y. Weiss, and M. Jordan,
Loopy-belief Propagation for Approximate Inference:
An Empirical Study, UAI 1999."

LBP has a wide range of applications in structured prediction, such as
low-level vision and influence spread in social networks, where we have prior
noisy predictions for a large set of random variables and a graph encoding
relationships between those variables.

The algorithm performs approximate inference on an :term:`undirected graph` of
hidden variables, where each variable is represented as a node, and each edge
encodes relations to its neighbors.
Initially, a prior noisy estimate of state probabilities is given to each
node, then the algorithm infers the posterior distribution of each node by
propagating and collecting messages to and from its neighbors and updating
the beliefs.

In graphs containing loops, convergence is not guaranteed, though LBP has
demonstrated empirical success in many areas and in practice often converges
close to the true joint probability distribution.

**Discrete Loopy Belief Propagation**

LBP is typically considered a :term:`semi-supervised machine learning
<Semi-Supervised Learning>` algorithm as

1)  there is typically no ground truth observation of states and
#)  the algorithm is primarily concerned with estimating a joint
    probability function rather than
    with :term:`classification` or point prediction.

The standard (discrete) LBP algorithm requires a set of probability thresholds
to be considered a classifier.
Nonetheless, the discrete LBP algorithm allows Test/Train/Validate splits of
the data and the algorithm will treat "Train" observations
differently from "Test" and "Validate" observations.
Vertices labelled with "Test" or "Validate" will be treated as though they have
uninformative (uniform) priors and are
allowed to receive messages, but not send messages.
This simulates a "scoring scenario" in which a new observation is added to a
graph containing fully trained LBP posteriors, the new vertex is scored based
on received messages, but the full LBP algorithm is not repeated in full.
This behavior can be turned off by setting the ``ignore_vertex_type`` parameter
to True.
When ``ignore_vertex_type=True``, all nodes will be considered "Train"
regardless of their sample type designation.
The Gaussian (continuous) version of LBP does not allow Train/Test/Validate
splits.

The standard LBP algorithm included with the toolkit assumes an ordinal and
cardinal set of discrete states.
For notational convenience, we'll denote the value of state :math:`s_{i}` as
:math:`i`, and the prior probability of state
:math:`s_{i}` as :math:`prior_{i}`.

Each node sends out initial messages of the form:

.. math::

   \ln \left ( \sum_{s_{j}} \exp \left ( - \frac { | i - j | ^{p} }{ n - 1 } \
   * w * s + \ln (prior_{i}) \right ) \right )

Where :math:`w` is the weight between the messages destination and
origin vertices, :math:`s` is the :term:`smoothing` parameter,
:math:`p` is the power parameter, and :math:`n` is the number of states.
The larger the weight between two nodes or the higher the smoothing parameter,
the more neighboring vertices are assumed to "agree" on states.
(Here, we represent messages as sums of log probabilities rather than products
of non-logged probabilities which makes it easier to subtract messages in the
future steps of the algorithm.)
Also note that the states are cardinal in the sense that the "pull" of state
:math:`i` on state :math:`j` depends on the distance between :math:`i` and
:math:`j`.
The *power* parameter intensifies the rate at which the pull of distant states
drops off.

In order for the algorithm to work properly, all edges of the graph must be
bidirectional.
In other words, messages need to be able to flow in both directions across
every edge.
Bidirectional edges can be enforced during graph building, but the LBP function
provides an option to do an initial check for bidirectionality using the
``bidirectional_check=True`` option.
If not all the edges of the graph are bidirectional, the algorithm will return
an error.

Look at a case where a node has two states, 0 and 1.
The 0 state has a prior probability of 0.9 and the 1 state has a prior
probability of 0.2.
The states have uniform weights of 1, power of 1 and a smoothing parameter of
2.
The nodes initial message would be
:math:`\textstyle \left [ \ln \left ( 0.2 + 0.8 e ^{-2} \right ), \ln \left ( \
0.8 + 0.2 e ^{-2} \right ) \right ]`,
which gets sent to each of that node's neighbors.
Note that messages will typically not be proper probability distributions,
hence each message is normalized so that the probability of all states sum to 1
before being sent out.
For simplicity, we will consider all messages going forward as normalized
messages.

After nodes have sent out their initial messages, they then update their
beliefs based on messages that they have received from their neighbors,
denoted by the set :math:`k`.

Updated Posterior Beliefs:

.. math::

   \ln (newbelief) = \propto \exp \left [ \ln (prior) + \sum_k message _{k} \
   \right ]

Note that the messages in the above equation are still in log form.
Nodes then send out new messages which take the same form as their initial
messages,
with updated beliefs in place of priors and subtracting out the information
previously received from the new message's recipient.
The recipient's prior message is subtracted out to prevent feedback loops of
nodes "learning" from themselves.

In updating beliefs, new beliefs tend to be most influenced by the largest
message.
Setting the ``max_product`` option to "True" ignores all incoming messages
other than the strongest signal.
Doing this results in approximate solutions, but requires significantly less
memory and run-time than the more exact computation.
Users should consider this option when processing power is a constraint and
approximate solutions to LBP will be sufficient.

.. math::

   \ln \left ( \sum_{s_{j}} \exp \left ( - \frac { | i - j | ^{p} }{ n - 1 } \
   * w * s + \ln (newbelief_{i}) - \
   previous\ message\ from\ recipient \right ) \right )

This process of updating and message passing continues until the convergence
criteria is met or the maximum number of :term:`supersteps` is
reached without converging.
A node is said to converge if the total change in its distribution (the sum of
absolute value changes in state probabilities) is less than
the ``convergence_threshold`` parameter.
Convergence is a local phenomenon; not all nodes will converge at the same
time.
It is also possible for some (most) nodes to converge and others to never
converge.
The algorithm requires all nodes to converge before declaring that the
algorithm has converged overall.
If this condition is not met, the algorithm will continue up to the maximum
number of :term:`supersteps`.
""",
  returns = """a 2-column frame:

    vertex: int
        A vertex id.
    result : Vector (long)
        label vector for the results (for the node id in column 1).""")
class LoopyBeliefPropagationPlugin
    extends CommandPlugin[LoopyBeliefPropagationArgs, LoopyBeliefPropagationResult] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/loopy_belief_propagation"

  override def execute(arguments: LoopyBeliefPropagationArgs)(implicit invocation: Invocation): LoopyBeliefPropagationResult = {
    val frames = engine.frames

    //TODO validate frame args here
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(EngineConfig.config, "trustedanalytics.atk.engine.giraph")
    val giraphConf = new LoopyBeliefPropagationConfiguration(hadoopConf)

    val outputFrame = frames.create(CreateEntityArgs(description = Some("Loopy belief propagation results")))
    val outputFrameSaveInfo = frames.prepareForSave(outputFrame)
    val inputFormatConfig = new LoopyBeliefPropagationInputFormatConfig(frame.getStorageLocation, frame.schema)
    val outputFormatConfig = new LoopyBeliefPropagationOutputFormatConfig(outputFrameSaveInfo.targetPath)
    val loopyBeliefPropagationConfig = new LoopyBeliefPropagationConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(loopyBeliefPropagationConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)
    GiraphConfigurationUtil.set(giraphConf, "mapreduce.input.fileinputformat.inputdir", Some(inputFormatConfig.parquetFileLocation))

    giraphConf.setEdgeInputFormatClass(classOf[LoopyBeliefPropagationEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LoopyBeliefPropagationVertexOutputFormat])
    giraphConf.setVertexInputFormatClass(classOf[LoopyBeliefPropagationVertexInputFormat])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationAggregatorWriter])

    val result = GiraphJobManager.run("ia_giraph_lbp",
      classOf[LoopyBeliefPropagationComputation].getCanonicalName,
      giraphConf,
      invocation,
      "lbp-learning-report")

    val resultsColumn = Column(arguments.srcLabelColName, frame.schema.columnDataType(arguments.srcLabelColName))
    frames.postSave(outputFrame.toReference, outputFrameSaveInfo, new FrameSchema(List(frame.schema.column(arguments.srcColName), resultsColumn)))

    LoopyBeliefPropagationResult(frames.expectFrame(outputFrame.toReference), result)
  }

}
