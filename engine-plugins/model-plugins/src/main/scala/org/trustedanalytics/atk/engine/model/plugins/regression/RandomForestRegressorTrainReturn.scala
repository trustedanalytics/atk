/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.model.plugins.regression

/**
 * Results for Random Forest Classifier Train plugin
 *
 * @param observationColumns List of column name(s) on which the model was trained
 * @param labelColumn Column name containing the true label for each observation
 * @param numTrees Number of trees in the trained random forest model
 * @param numNodes Number of nodes in the trained random forest model
 * @param featureSubsetCategory Number of features to consider for splits at each node
 * @param impurity Criterion used for information gain calculation
 * @param maxDepth Maximum depth of the tree
 * @param maxBins Maximum number of bins used for splitting features
 * @param seed Random seed for bootstrapping and choosing feature subsets
 */
case class RandomForestRegressorTrainReturn(observationColumns: List[String],
                                            labelColumn: String,
                                            numTrees: Int,
                                            numNodes: Int,
                                            featureSubsetCategory: String,
                                            impurity: String,
                                            maxDepth: Int,
                                            maxBins: Int,
                                            seed: Int)
