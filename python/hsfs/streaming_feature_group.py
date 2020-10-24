#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import humps
import json

from hsfs import util, kafka_topic
from hsfs.core import streaming_feature_group_engine


class StreamingFeatureGroup:
    def __init__(
        self,
        name,
        version,
        description,
        featurestore_id,
        topic=None,
        application_id=None,
        featurestore_name=None,
        created=None,
        creator=None,
        id=None,
        features=None,
    ):
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = creator
        self._version = version
        self._name = name
        self._application_id = application_id

        if topic:
            self._topic = kafka_topic.KafkaTopic.from_response_json(topic)

        self._streaming_feature_group_engine = streaming_feature_group_engine.StreamingFeatureGroupEngine(
            featurestore_id
        )

    def save(self, method):
        self._method = method
        self._streaming_feature_group_engine.save(self)

    def apply(self, stream_options={}):
        self._streaming_feature_group_engine.apply(self, stream_options)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "method": self._method,
            "application_id": self._application_id,
            "type": "streamingFeaturegroupDTO",
        }

    @property
    def method(self):
        return self._method

    @property
    def kafka_topic_name(self):
        return self._kafka_topic_name

    @property
    def application_id(self):
        return self._application_id
