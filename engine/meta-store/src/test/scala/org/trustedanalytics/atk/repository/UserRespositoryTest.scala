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

import org.trustedanalytics.atk.domain.UserTemplate
import org.scalatest.Matchers

class UserRespositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "UserRepository" should "be able to create users" in {

    val userRepo = slickMetaStoreComponent.metaStore.userRepo

    slickMetaStoreComponent.metaStore.withSession("user-test") {
      implicit session =>

        val apiKey = "my-api-key-" + System.currentTimeMillis()

        // create a user
        val user = userRepo.insert(new UserTemplate(apiKey))
        user.get.apiKey shouldBe Some(apiKey)

        // look it up and validate expected values
        val user2 = userRepo.lookup(user.get.id)
        user.get shouldBe user2.get
        user.get.createdOn should not be null
        user.get.modifiedOn should not be null
    }
  }
}
