package org.apache.spark.mllib.evaluation

import breeze.linalg.{ DenseMatrix => BDM }

trait HessianMatrix {

  /**
   * Get the optional Hessian matrix for the model
   */
  def getHessianMatrix: Option[BDM[Double]]
}
