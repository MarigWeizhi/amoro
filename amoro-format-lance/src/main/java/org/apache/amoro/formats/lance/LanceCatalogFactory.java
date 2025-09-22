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

import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.dir.DirectoryNamespace;
import com.lancedb.lance.namespace.rest.RestNamespace;
import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.table.TableMetaStore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LanceCatalogFactory implements FormatCatalogFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogFactory.class);

  public static final String PAIMON_S3_ACCESS_KEY = "s3.access-key";
  public static final String PAIMON_S3_SECRET_KEY = "s3.secret-key";
  public static final String PAIMON_OSS_ACCESS_KEY = "fs.oss.accessKeyId";
  public static final String PAIMON_OSS_SECRET_KEY = "fs.oss.accessKeySecret";
  public static final String PAIMON_OSS_ENDPOINT = "fs.oss.endpoint";

  @Override
  public LanceCatalog create(
      String name, String metastoreType, Map<String, String> properties, TableMetaStore metaStore) {
    LanceNamespace lanceNamespace;
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    switch (metastoreType) {
      case "dir":
        lanceNamespace = new DirectoryNamespace();
        break;
      case "rest":
        lanceNamespace = new RestNamespace();
        break;
      default:
        throw new IllegalArgumentException("Unsupported metastore type: " + metastoreType);
    }
    lanceNamespace.initialize(properties, allocator);
    return new LanceCatalog(lanceNamespace, name);
  }

  @Override
  public TableFormat format() {
    return TableFormat.LANCE;
  }

  @Override
  public Map<String, String> convertCatalogProperties(
      String catalogName, String metastoreType, Map<String, String> unifiedCatalogProperties) {
    // TODO
    return unifiedCatalogProperties;
  }
}
