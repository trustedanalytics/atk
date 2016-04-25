package org.trustedanalytics.atk.scoring.models

/**
 * DAAL Naive Bayes model
 *
 * @param serializedModel Serialized Naive Bayes model
 * @param observationColumns List of column(s) storing the observations
 * @param labelColumn Column name containing the label
 * @param numClasses Number of classes
 * @param alpha Imagined occurrences of features
 */
case class DaalNaiveBayesModelData(serializedModel: List[Byte],
                                   observationColumns: List[String],
                                   labelColumn: String,
                                   numClasses: Int,
                                   alpha: Double)
