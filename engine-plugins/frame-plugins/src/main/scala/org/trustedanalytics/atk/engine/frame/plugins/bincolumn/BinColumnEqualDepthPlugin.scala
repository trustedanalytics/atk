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

package org.trustedanalytics.atk.engine.frame.plugins.bincolumn

import org.trustedanalytics.atk.domain.frame.{ ComputedBinColumnArgs, Missing }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 *
 */
@PluginDoc(oneLine = "Classify column into groups with the same frequency.",
  extended = """Group rows of data based on the value in a single column and add a label
to identify grouping.

Equal depth binning attempts to label rows such that each bin contains the
same number of elements.
For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin
number is determined by:

.. math::

    \lceil n * \frac { f(C) }{ m } \rceil

where :math:`f` is a tie-adjusted ranking function over values of
:math:`C`.
If there are multiples of the same value in :math:`C`, then their
tie-adjusted rank is the average of their ordered rank values.

**Notes**

#)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
#)  The num_bins parameter is considered to be the maximum permissible number
    of bins because the data may dictate fewer bins.
    For example, if the column to be binned has a quantity of :math"`X`
    elements with only 2 distinct values and the *num_bins* parameter is
    greater than 2, then the actual number of bins will only be 2.
    This is due to a restriction that elements with an identical value must
    belong to the same bin.""",
  returns = "A list containing the edges of each bin.")
class BinColumnEqualDepthPlugin extends ComputedBinColumnPlugin {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  override def name: String = "frame/bin_column_equal_depth"

  /**
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @param invocation
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: ComputedBinColumnArgs)(implicit invocation: Invocation): Int = 8

  /**
   * Discretize a variable into a finite number of bins
   * @param columnIndex index of column to bin
   * @param numBins number of bins to use
   * @param missing specifies the behavior of missing values in the bin column.  Either ignore missing values
   *                 (and bin them to -1), or specify a value to use when binning elements with a missing value.
   * @param rdd rdd to bin against
   * @return a result object containing the binned rdd and the list of computed cutoffs
   */
  override def executeBinColumn(columnIndex: Int, numBins: Int, missing: Missing[Any], rdd: FrameRdd): RddWithCutoffs = {
    DiscretizationFunctions.binEqualDepth(columnIndex, numBins, None, rdd)
  }
}
