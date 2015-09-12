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

import org.trustedanalytics.atk.domain.frame.DataFrameTemplate
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphTemplate }
import org.joda.time.DateTime
import org.scalatest.Matchers

class FrameRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "FrameRepository" should "be able to create frames" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val frameName = Some("frame_name")
        val frameDescription = "my description"

        // create a frame
        val frame = frameRepo.insert(new DataFrameTemplate(frameName, Some(frameDescription)))
        frame.get should not be null

        // look it up and validate expected values
        val frame2 = frameRepo.lookup(frame.get.id)
        frame2.get should not be null
        frame2.get.name shouldBe frameName
        frame2.get.description.get shouldBe frameDescription
        frame2.get.status shouldBe ActiveStatus
        frame2.get.createdOn should not be null
        frame2.get.modifiedOn should not be null
    }

  }

  it should "be able to update errorFrameIds in" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val frameName = "frame_name"

        // create the frames
        val frame = frameRepo.insert(new DataFrameTemplate(Some(frameName), None)).get
        val errorFrame = frameRepo.insert(new DataFrameTemplate(Some(frameName + "_errors"), None)).get

        // invoke method under test
        frameRepo.updateErrorFrameId(frame, Some(errorFrame.id))

        // look it up and validate expected values
        val frame2 = frameRepo.lookup(frame.id)
        frame2.get.errorFrameId.get shouldBe errorFrame.id
    }
  }

  it should "return a list of frames ready to have their data deleted" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val frameName = Some("frame_name")

        // create the frames
        // should not be in list    too new
        val frame = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame.copy(lastReadDate = new DateTime))

        //should be in list old and unreferenced
        val frame2 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should be in list old things that are referenced can be weakly live
        val frame3 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame3.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val frame4 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame4.copy(lastReadDate = new DateTime().minus(age * 2), parent = Some(frame3.id)))

        val seamlessWeak: GraphEntity = graphRepo.insert(new GraphTemplate(None)).get
        val seamlessLive: GraphEntity = graphRepo.insert(new GraphTemplate(Some("liveGraph"))).get

        //should not be in list. it is too new
        val frame5 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame5.copy(lastReadDate = new DateTime(), graphId = Some(seamlessWeak.id)))

        //should be in list. it is old and referenced by a weakly live graph
        val frame6 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame6.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessWeak.id)))

        //should not be in list. it is old but referenced by a live graph
        val frame7 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame7.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessLive.id)))

        //should be in list. User has marked as ready to delete
        val frame8 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame8.copy(status = DeletedStatus))

        // should not be in list. Has already been deleted
        val frame9 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame9.copy(status = DeletedFinalStatus))

        val staleEntities = frameRepo.getStaleEntities(age)
        val idList = staleEntities.map(f => f.id).toList
        idList should contain(frame2.id)
        idList should contain(frame3.id)
        idList should contain(frame6.id)
        idList should contain(frame8.id)
        staleEntities.length should be(4)
    }
  }
}
