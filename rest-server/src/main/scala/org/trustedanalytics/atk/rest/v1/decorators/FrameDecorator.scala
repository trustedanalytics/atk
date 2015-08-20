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

package org.trustedanalytics.atk.rest.v1.decorators

import org.trustedanalytics.atk.domain.Status
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.rest.v1.viewmodels.{ Rel, RelLink, GetDataFrame, GetDataFrames }
import spray.http.Uri
import org.apache.commons.lang.StringUtils
import spray.json.JsString

/**
 * A decorator that takes an entity from the database and converts it to a View/Model
 * for delivering via REST services
 */
object FrameDecorator extends EntityDecorator[FrameEntity, GetDataFrames, GetDataFrame] {

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * Self-link and errorFrame links are auto-created from supplied uri.
   *
   * @param uri the uri to the current entity
   * @param additionalLinks related links
   * @param entity the entity to decorate
   * @return the View/Model
   */
  override def decorateEntity(uri: String, additionalLinks: Iterable[RelLink] = Nil, entity: FrameEntity): GetDataFrame = {

    var links = List(Rel.self(uri.toString)) ++ additionalLinks

    if (entity.errorFrameId.isDefined) {
      val baseUri = StringUtils.substringBeforeLast(uri, "/")
      links = RelLink("ia-error-frame", baseUri + "/" + entity.errorFrameId.get, "GET") :: links
    }

    GetDataFrame(uri = entity.uri,
      name = entity.name,
      schema = entity.schema,
      rowCount = entity.rowCount,
      links,
      getFrameUri(entity.errorFrameId),
      entity.entityType,
      Status.getName(entity.status))
  }

  def getFrameUri(id: Option[Long]): Option[String] = {
    id match {
      case Some(number) => Some(s"frames/$number")
      case None => None
    }
  }

  def decorateEntities(uri: String, additionalLinks: Iterable[RelLink] = Nil, entities: Seq[FrameEntity]): List[GetDataFrame] = {
    entities.map(frame => decorateEntity(uri, additionalLinks, frame)).toList
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[FrameEntity]): List[GetDataFrames] = {
    entities.map(frame => new GetDataFrames(id = frame.id,
      name = frame.name,
      url = uri + "/" + frame.id,
      entityType = frame.entityType)).toList
  }

}
