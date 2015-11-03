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


package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.gc.{ GarbageCollectionTemplate, GarbageCollectionEntry, GarbageCollectionEntryTemplate, GarbageCollection }
import org.joda.time.DateTime
import org.scalatest.Matchers

class GarbageCollectionEntryRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "GarbageCollectionEntryRepository" should "be able to create new items" in {
    val gcEntryRepo = slickMetaStoreComponent.metaStore.gcEntryRepo
    slickMetaStoreComponent.metaStore.withSession("gc_entry-test") {
      implicit session =>

        //create gc to use for foreign key reference
        val startTime: DateTime = new DateTime
        val gc = slickMetaStoreComponent.metaStore.gcRepo.insert(new GarbageCollectionTemplate("hostname", 100, startTime))
        val dropStaleTemplate = new GarbageCollectionEntryTemplate(gc.get.id, "description: drop stale", startTime)
        val finalizeDroppedTemplate = new GarbageCollectionEntryTemplate(gc.get.id, "description: finalize dropped", startTime)

        val entry = gcEntryRepo.insert(dropStaleTemplate)
        entry.get should not be null

        //look it up
        val entry2: Option[GarbageCollectionEntry] = gcEntryRepo.lookup(entry.get.id)
        entry2.get should not be null
        entry2.get.garbageCollectionId should be(gc.get.id)
        entry2.get.description should be("description: drop stale")
        entry2.get.startTime.getMillis should be(startTime.getMillis)
        entry2.get.endTime should be(None)

        // shouldn't have a problem adding a another entry
        val entryFinalize = gcEntryRepo.insert(finalizeDroppedTemplate)
        entryFinalize.get should not be null

    }

  }

}
