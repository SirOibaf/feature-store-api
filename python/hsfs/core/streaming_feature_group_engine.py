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

from hsfs import engine
from hsfs.core import streaming_feature_group_api


class StreamingFeatureGroupEngine:
    def __init__(self, feature_store_id):
        self._streaming_feature_group_api = streaming_feature_group_api.StreamingFeatureGroupApi(
            feature_store_id
        )

    def save(self, feature_group):
        # TODO(Fabio): check that the signature matches
        self._streaming_feature_group_api.save(feature_group)

    def apply(self, feature_group, stream_options):
        engine.get_instance().setup_stream("topic_name", stream_options)