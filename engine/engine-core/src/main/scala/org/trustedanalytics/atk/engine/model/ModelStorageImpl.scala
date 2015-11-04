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

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.{ DuplicateNameException, EventLoggingImplicits, NotFoundException }
import org.trustedanalytics.atk.domain.model._
import org.trustedanalytics.atk.domain.{ Status, CreateEntityArgs }
import org.trustedanalytics.atk.engine.ModelStorage
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.repository.MetaStore
import spray.json.JsObject
/**
 * Front end for Spark to create and manage models.
 * @param metaStore Repository for model meta data.
 */

class ModelStorageImpl(metaStore: MetaStore)
    extends ModelStorage
    with EventLogging
    with EventLoggingImplicits {

  /** Lookup a Model, Throw an Exception if not found */
  override def expectModel(modelRef: ModelReference): ModelEntity = {
    metaStore.withSession("spark.modelstorage.lookup") {
      implicit session =>
        {
          metaStore.modelRepo.lookup(modelRef.id)
        }
    }.getOrElse(throw new NotFoundException("model", modelRef.id))
  }

  /**
   * Registers a new model.
   * @param createArgs arguments to create the model entity
   * @return Model metadata.
   */
  override def createModel(createArgs: CreateEntityArgs)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.create") {
      implicit session =>
        {
          if (createArgs.name.isDefined) {
            if (metaStore.modelRepo.lookupByName(Some(createArgs.name.get)).isDefined) {
              throw new DuplicateNameException("model", createArgs.name.get, "Model with same name already exists. Create aborted.")
            }
            else if (metaStore.graphRepo.lookupByName(Some(createArgs.name.get)).isDefined) {
              throw new DuplicateNameException("graph", createArgs.name.get, "Graph with same name already exists. Create aborted.")
            }
            else if (metaStore.frameRepo.lookupByName(Some(createArgs.name.get)).isDefined) {
              throw new DuplicateNameException("frame", createArgs.name.get, "Frame with same name already exists. Create aborted.")
            }
          }
          val modelTemplate = ModelTemplate(createArgs.name, createArgs.entityType.get)
          metaStore.modelRepo.insert(modelTemplate).get
        }
    }
  }

  /**
   * Renames a model in the metastore.
   * @param modelRef The model being renamed
   * @param newName The name the model is being renamed to.
   * @return Model metadata
   */
  override def renameModel(modelRef: ModelReference, newName: String): ModelEntity = {
    metaStore.withSession("spark.modelstorage.rename") {
      implicit session =>
        {
          if (metaStore.modelRepo.lookupByName(Some(newName)).isDefined) {
            throw new DuplicateNameException("model", newName, "Model with same name exists. Rename aborted.")
          }
          else if (metaStore.graphRepo.lookupByName(Some(newName)).isDefined) {
            throw new DuplicateNameException("graph", newName, "Graph with same name exists. Rename aborted.")
          }
          else if (metaStore.frameRepo.lookupByName(Some(newName)).isDefined) {
            throw new DuplicateNameException("frame", newName, "Frame with same name exists. Rename aborted.")
          }

          val newModel = expectModel(modelRef).copy(name = Some(newName))
          metaStore.modelRepo.update(newModel).get
        }
    }
  }

  override def getModelByName(name: Option[String]): Option[ModelEntity] = {
    metaStore.withSession("spark.modelstorage.getModelByName") {
      implicit session =>
        {
          metaStore.modelRepo.lookupByName(name)
        }
    }
  }

  /**
   * Obtain the model metadata for a range of model IDs.
   * @return Sequence of model metadata objects.
   */
  override def getModels()(implicit invocation: Invocation): Seq[ModelEntity] = {
    metaStore.withSession("spark.modelstorage.getModels") {
      implicit session =>
        {
          metaStore.modelRepo.scanNamedActiveModelsNoData()
        }
    }
  }

  /**
   * Store the result of running the train data on a model
   * @param modelRef The model to update
   * @param newData JsObject storing the result of training.
   */
  override def updateModel(modelRef: ModelReference, newData: JsObject)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.updateModel") {
      implicit session =>
        {
          val currentModel = expectModel(modelRef)
          val newModel = currentModel.copy(data = Option(newData))

          val updatedModel = metaStore.modelRepo.update(newModel).get
          updateLastReadDate(updatedModel)
        }
    }
  }

  /**
   * Update last read date of the model
   * @param model The model to update
   */
  override def updateLastReadDate(model: ModelEntity)(implicit invocation: Invocation): ModelEntity = {
    metaStore.withSession("spark.modelstorage.updateLastReadDate") {
      implicit session =>
        {
          metaStore.modelRepo.updateLastReadDate(model).get
        }
    }
  }

  /**
   * Mark model as Dropped
   * @param model model to drop
   * @param invocation current invocation
   */
  def dropModel(model: ModelEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.modelstorage.dropModel") {
      implicit session =>
        {
          info(s"marking model entity (id=${model.id}, name=${model.name}) as dropped")
          metaStore.modelRepo.dropModel(model)
        }
    }
  }
}
