/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.formats.lance;

import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class LanceHiveCatalogTestHelper extends LanceDirectoryCatalogTestHelper {

  public LanceHiveCatalogTestHelper(String catalogName, Map<String, String> catalogProperties) {
    super(catalogName, catalogProperties);
  }

  @Override
  public void initHiveConf(Configuration hiveConf) {
    // TODO
    catalogProperties.put("hive2", "");
  }

  protected String getMetastoreType() {
    return CatalogMetaProperties.CATALOG_TYPE_HIVE;
  }

  public static LanceHiveCatalogTestHelper defaultHelper() {
    return new LanceHiveCatalogTestHelper("test_paimon_catalog", new HashMap<>());
  }
}
