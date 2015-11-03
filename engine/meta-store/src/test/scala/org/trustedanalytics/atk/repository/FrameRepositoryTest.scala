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

        frameRepo.isErrorFrame(frame) shouldBe false
        frameRepo.isErrorFrame(errorFrame) shouldBe true
    }
  }

  it should "detect stale frames and dropped frames" in {
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

        //should not be in list since it is owned by frame 4, as an error frame
        val frame3 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame3.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val frame4 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame4.copy(lastReadDate = new DateTime().minus(age * 2), errorFrameId = Some(frame3.id)))

        val seamlessNoName: GraphEntity = graphRepo.insert(new GraphTemplate(None)).get
        val seamlessWithName: GraphEntity = graphRepo.insert(new GraphTemplate(Some("named_graph"))).get

        //should not be in list.  It is too new
        val frame5 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame5.copy(lastReadDate = new DateTime(), graphId = Some(seamlessNoName.id)))

        //should not be in list.  Though it is old and has no name, it referenced by another entity seamlessNoName
        val frame6 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame6.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessNoName.id)))

        //should not be in list.  Though it is old and has no name, it referenced by another entity seamlessWithName
        val frame7 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame7.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessWithName.id)))

        // should not be in the stale list.  User has dropped it
        val frame8 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame8.copy(status = DroppedStatus))

        // should not be in the stale list. Has already been finalized
        val frame9 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame9.copy(status = FinalizedStatus))

        frameRepo.isErrorFrame(frame2) shouldBe false
        frameRepo.isErrorFrame(frame3) shouldBe true

        val stale = frameRepo.getStaleEntities(age).map(f => f.id).toList
        stale should contain(frame2.id)
        stale.length should be(1)

        val dropped = frameRepo.droppedFrames.map(f => f.id).toList
        dropped should contain(frame8.id)
        dropped.length should be(1)
    }
  }
}
