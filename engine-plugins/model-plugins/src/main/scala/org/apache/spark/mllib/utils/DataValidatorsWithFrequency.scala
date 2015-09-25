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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.utils

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.LabeledPointWithFrequency
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * A collection of methods used to validate data before applying ML algorithms.
 *
 * Extension of MlLib's data validators that supports a frequency column.
 * The frequency column contains the frequency of occurrence of each observation.
 * @see org.apache.spark.mllib.utils.DataValidators
 *
 */
@DeveloperApi
object DataValidatorsWithFrequency extends Logging {

  /**
   * Function to check if labels used for classification are either zero or one.
   *
   * @return True if labels are all zero or one, false otherwise.
   */
  val binaryLabelValidator: RDD[LabeledPointWithFrequency] => Boolean = { data =>
    val numInvalid = data.filter(x => x.label != 1.0 && x.label != 0.0).count()
    if (numInvalid != 0) {
      throw new IllegalArgumentException("Classification labels should be 0 or 1. Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }

  /**
   * Function to check if labels used for k class multi-label classification are
   * in the range of {0, 1, ..., k - 1}.
   *
   * @return True if labels are all in the range of {0, 1, ..., k-1}, false otherwise.
   */
  def multiLabelValidator(k: Int): RDD[LabeledPointWithFrequency] => Boolean = { data =>
    val numInvalid = data.filter(x =>
      x.label - x.label.toInt != 0.0 || x.label < 0 || x.label > k - 1).count()
    if (numInvalid != 0) {
      throw new IllegalArgumentException("Classification labels should be in {0 to " + (k - 1) + "}. " +
        "Found " + numInvalid + " invalid labels")
    }
    numInvalid == 0
  }
}
