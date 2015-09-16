#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Model
"""

import logging
import json
logger = logging.getLogger(__name__)
from trustedanalytics.meta.clientside import *
api = get_api_decorator(logger)

from trustedanalytics.meta.namedobj import name_support
from trustedanalytics.meta.metaprog import CommandInstallable as CommandLoadable
from trustedanalytics.meta.docstub import doc_stubs_import
from trustedanalytics.rest.atkserver import server

# _BaseModel
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubs_BaseModel
    doc_stubs_import.success(logger, "_DocStubs_BaseModel")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubs_BaseModel", e)
    class _DocStubs_BaseModel(object): pass


@api
@name_support('model')
class _BaseModel(_DocStubs_BaseModel, CommandLoadable):
    """
    Class with information about a model.
    Has information needed to modify data and table structure.

    Parameters
    -----------
    name: string
        The name of the newly created model

    Returns
    -------
    Model
        An object with access to the model
    """
    _entity_type = 'model'

    def __init__(self):
        self.uri = None
        CommandLoadable.__init__(self)

    def _get_model_info(self):
        response = server.get(self._get_model_full_uri())
        return ModelInfo(response.json())

    def _get_model_full_uri(self):
        return server.create_full_uri(self.uri)

    @staticmethod
    def _is_entity_info(obj):
        return isinstance(obj, ModelInfo)

    def __repr__(self):
        try:
            model_info = self._get_model_info()
            return "\n".join([self.__class__.__name__, 'name =  "%s"' % (model_info.name), "status = %s" % model_info.status])
        except:
            return super(_BaseModel,self).__repr__() + " (Unable to collect metadata from server)"

    def __eq__(self, other):
        if not isinstance(other, _BaseModel):
            return False
        return self.uri == other.uri


class ModelInfo(object):
    """
    JSON based Server description of a Model
    """
    def __init__(self, model_json_payload):
        self._payload = model_json_payload
        self._validate()

    def __repr__(self):
        return json.dumps(self._payload, indent =2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.uri, self.name)

    def _validate(self):
        try:
            assert self.uri
        except KeyError:
            raise RuntimeError("Invalid response from server. Expected Model info.")

    @property
    def name(self):
        return self._payload.get('name', None)

    @property
    def uri(self):
        return self._payload['uri']

    @property
    def links(self):
        return self._links['links']

    @property
    def status(self):
        return self._payload['status']

    def initialize_model(self, model):
        model.uri = self.uri

    def update(self,payload):
        if self._payload and self.uri != payload['uri']:
            msg = "Invalid payload, model ID mismatch %d when expecting %d" \
                  % (payload['uri'], self.uri)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
