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
package org.trustedanalytics.atk.domain

import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.scoring.ScoringEngineHelper
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }

class ScoringEngineHelperTest extends WordSpec with Matchers {
  val oldModel = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"))
    }

    override def modelMetadata(): ModelMetaDataArgs = {
      new ModelMetaDataArgs("Dummy Model", "Dummy Class", "Dummy Reader", Map("Created_On" -> "Jan 29th 2016"))
    }

    override def output(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"), Field("score", "double"))
    }

    override def score(row: Array[Any]): Array[Any] = ???
  }

  val revisedModelCompatible = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"))
    }

    override def modelMetadata(): ModelMetaDataArgs = {
      new ModelMetaDataArgs("Dummy Model", "Dummy Class", "Dummy Reader", Map("Created_On" -> "Jan 29th 2016"))
    }

    override def output(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"), Field("score", "double"))
    }

    override def score(row: Array[Any]): Array[Any] = ???
  }

  val revisedModelDifferentParam = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"))
    }

    override def modelMetadata(): ModelMetaDataArgs = {
      new ModelMetaDataArgs("Dummy Model", "Dummy Class", "Dummy Reader", Map("Created_On" -> "Jan 29th 2016"))
    }

    override def output(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("score", "double"))
    }

    override def score(row: Array[Any]): Array[Any] = ???
  }

  val revisedModelDifferentModelType = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"))
    }

    override def modelMetadata(): ModelMetaDataArgs = {
      new ModelMetaDataArgs("Diff Dummy Model", "Dummy Class", "Dummy Reader", Map("Created_On" -> "Jan 29th 2016"))
    }

    override def output(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"), Field("score", "double"))
    }

    override def score(row: Array[Any]): Array[Any] = ???
  }

  "ScoringEngineHelper" should {
    "successfully compare two compatible models" in {
      assert(ScoringEngineHelper.isModelCompatible(oldModel, revisedModelCompatible))
    }
  }

  "ScoringEngineHelper" should {
    "fail on comparing two models with difference input output parameters" in {
      assert(!ScoringEngineHelper.isModelCompatible(oldModel, revisedModelDifferentParam))
    }
  }

  "ScoringEngineHelper" should {
    "successfully compare two models with difference input output parameters when force=true" in {
      val force = true
      assert(ScoringEngineHelper.isModelCompatible(oldModel, revisedModelDifferentParam, force))
    }
  }

  "ScoringEngineHelper" should {
    "fail on comparing two models with difference model type" in {
      assert(!ScoringEngineHelper.isModelCompatible(oldModel, revisedModelDifferentModelType))
    }
  }

}
