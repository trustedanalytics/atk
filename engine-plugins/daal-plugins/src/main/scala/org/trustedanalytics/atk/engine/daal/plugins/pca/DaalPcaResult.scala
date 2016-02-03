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
package org.trustedanalytics.atk.engine.daal.plugins.pca

import com.intel.daal.algorithms.pca.{ ResultId, Result }
import com.intel.daal.data_management.data.NumericTable

/**
 * Class for PCA results
 * scores - A nx1 NumericTable of Eigen values, sorted from largest
 * to the smallest.
 * loadings - A nxp NumericTable of corresponding Eigen vectors.
 */
case class DaalPcaResult() {
  private var scores: NumericTable = null
  private var loadings: NumericTable = null

  def this(scores: NumericTable, loadings: NumericTable) = {
    this()
    this.scores = scores
    this.loadings = loadings
  }

  def this(res: Result) = {
    this()
    scores = res.get(ResultId.eigenValues)
    loadings = res.get(ResultId.eigenVectors)
    scores.pack()
    loadings.pack()
  }

  def getScores: NumericTable = scores

  def getLoadings: NumericTable = loadings
}
