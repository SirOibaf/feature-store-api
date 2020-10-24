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

from hsfs import util


class KafkaTopic:
    def __init__(
        self,
        name,
        num_of_replicas,
        num_of_partitions,
        schema_name,
        schema_version,
        schema_content,
        owner_project_id,
        is_shared,
        accepted,
    ):
        self._name = name
        self._num_of_replicas = num_of_replicas
        self._num_of_partitions = num_of_partitions
        self._schema_name = schema_name
        self._schema_version = schema_version
        self._schema_content = schema_content
        self._owner_project_id = owner_project_id
        self._is_shared = is_shared
        self._accepted = accepted

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        _ = json_decamelized.pop("href")
        _ = json_decamelized.pop("count")
        return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "num_of_replicas": self._num_of_replicas,
            "num_of_partitions": self._num_of_partitions,
            "schema_name": self._schema_name,
            "schema_version": self._schema_version,
            "schema_content": self._schema_content,
            "owner_project_id": self._owner_project_id,
            "is_shared": self._is_shared,
            "accepted": self._accepted,
        }

    @property
    def name(self):
        return self._name
