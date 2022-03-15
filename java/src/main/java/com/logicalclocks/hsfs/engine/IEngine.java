/*
 * Copyright (c) 2022 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;

import java.io.IOException;
import java.util.Map;

public interface IEngine {

  public void validateEnvironment() throws FeatureStoreException;

  public Object sql(String query);

  public Object registerOnDemandTemporaryTable(OnDemandFeatureGroup onDemandFeatureGroup, String alias)
      throws FeatureStoreException, IOException;

  public void registerHudiTemporaryTable(FeatureGroup featureGroup, String alias, Long leftFeaturegroupStartTimestamp,
                                         Long leftFeaturegroupEndTimestamp, Map<String, String> readOptions);
}
