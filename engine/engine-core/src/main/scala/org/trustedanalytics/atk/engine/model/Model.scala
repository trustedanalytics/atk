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

package org.trustedanalytics.atk.engine.model

import org.trustedanalytics.atk.domain.model.{ ModelEntity, ModelReference }
import org.trustedanalytics.atk.engine.ModelStorage
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation }
import spray.json.JsObject

/**
 * Model interface for plugin authors
 */
trait Model {

  @deprecated("it is better if plugin authors do not have direct access to entity")
  def entity: ModelEntity

  /** name assigned by user for this model instance */
  def name: Option[String]

  def name_=(updatedName: String): Unit

  /** the type of the model eg: OLS, LogisticRegression */
  def modelType: String

  /** description of the model (a good default might say what frame it came from) */
  def description: Option[String]

  /** lifecycle status. For example, ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete) */
  def statusId: Long

  /**
   * the JsObject containing the trained model.
   * Expects model has been trained and data exists, throws appropriate exception otherwise
   */
  def data: JsObject

  def data_=(updatedData: JsObject): Unit

  /**
   * the JsObject containing the trained model
   */
  def dataOption: Option[JsObject]
}

object Model {

  implicit def modelToModelReference(model: Model): ModelReference = model.entity.toReference

  implicit def modelToModelEntity(model: Model): ModelEntity = model.entity

}

class ModelImpl(modelRef: ModelReference, modelStorage: ModelStorage)(implicit invocation: Invocation) extends Model {

  override def entity: ModelEntity = modelStorage.expectModel(modelRef)

  /** name assigned by user for this model instance */
  override def name: Option[String] = entity.name

  override def name_=(updatedName: String): Unit = modelStorage.renameModel(entity, updatedName)

  /** the type of the model eg: OLS, LogisticRegression */
  override def modelType: String = entity.modelType

  /** description of the model (a good default might say what frame it came from) */
  override def description: Option[String] = entity.description

  /** lifecycle status. For example, ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete) */
  override def statusId: Long = entity.statusId

  /**
   * Expects model has been trained and data exists, throws appropriate exception otherwise
   */
  override def data: JsObject = dataOption.getOrElse(throw new RuntimeException("Model has not yet been trained"))

  override def data_=(updatedData: JsObject): Unit = modelStorage.updateModel(modelRef, updatedData)

  override def dataOption: Option[JsObject] = {
    modelStorage.updateLastReadDate(entity)
    entity.data
  }

}
