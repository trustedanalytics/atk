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

import org.trustedanalytics.atk.domain.gc.{ GarbageCollection, GarbageCollectionTemplate }
import org.joda.time.DateTime
import org.scalatest.Matchers

class GarbageCollectionRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "GarbageCollectionRepository" should "be able to create new items" in {
    val gcRepo = slickMetaStoreComponent.metaStore.gcRepo
    slickMetaStoreComponent.metaStore.withSession("gc-test") {
      implicit session =>

        //create gc
        val startTime: DateTime = new DateTime
        val template = new GarbageCollectionTemplate("hostname", 100, startTime)

        val gc = gcRepo.insert(template)
        gc.get should not be null

        //look it up
        val gc2: Option[GarbageCollection] = gcRepo.lookup(gc.get.id)
        gc2.get should not be null
        gc2.get.hostname should be("hostname")
        gc2.get.processId should be(100)
        gc2.get.startTime.getMillis should be(startTime.getMillis)
        gc2.get.endTime should be(None)
    }

  }

}
