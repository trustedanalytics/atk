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

package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.model.ModelTemplate
import org.joda.time.DateTime
import org.scalatest.Matchers

class ModelEntityRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "ModelRepository" should "be able to create models" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val name = Some("my_model")
        val modelType = "model:logistic_regression"

        // create a model
        val model = modelRepo.insert(new ModelTemplate(name, modelType))
        model.get should not be null

        //look it up and validate expected values
        val model2 = modelRepo.lookup(model.get.id)
        model2.get should not be null
        model2.get.name shouldBe name
        model2.get.createdOn should not be null
        model2.get.modifiedOn should not be null
    }

  }

  it should "return a list of graphs ready to have their data deleted" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val name = Some("my_model")
        val modelType = "model:logistic_regression"

        // create graphs

        //should be in list old and unnamed
        val model1 = modelRepo.insert(new ModelTemplate(None, modelType)).get
        modelRepo.update(model1.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val model2 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is too new
        val model3 = modelRepo.insert(new ModelTemplate(None, modelType)).get
        modelRepo.update(model3.copy(lastReadDate = new DateTime()))

        //should  be in list as user marked it as deleted
        val model5 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model5.copy(statusId = DeletedStatus))

        //should not be in list it has already been deleted
        val model6 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model6.copy(statusId = DeletedFinalStatus))

        val readyForDeletion = modelRepo.listReadyForDeletion(age)
        readyForDeletion.length should be(2)
        val idList = readyForDeletion.map(m => m.id).toList
        idList should contain(model1.id)
    }
  }

}
