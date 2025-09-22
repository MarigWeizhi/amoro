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
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.AmoroCatalog;
import org.apache.amoro.TableFormat;
import org.apache.amoro.formats.AbstractFormatCatalogTestHelper;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceDirectoryCatalogTestHelper extends AbstractFormatCatalogTestHelper<LanceNamespace> {

  public LanceDirectoryCatalogTestHelper(String catalogName, Map<String, String> catalogProperties) {
    super(catalogName, catalogProperties);
  }

  public void initWarehouse(String warehouseLocation) {
    catalogProperties.put("root", warehouseLocation);
  }

  @Override
  protected TableFormat format() {
    return TableFormat.LANCE;
  }

  @Override
  protected String getMetastoreType() {
    return CatalogMetaProperties.CATALOG_TYPE_DIRECTORY;
  }

  @Override
  public AmoroCatalog amoroCatalog() {
    LanceCatalogFactory lanceCatalogFactory = new LanceCatalogFactory();
    TableMetaStore metaStore = CatalogUtil.buildMetaStore(getCatalogMeta());
    Map<String, String> paimonCatalogProperties =
        lanceCatalogFactory.convertCatalogProperties(
            catalogName, getMetastoreType(), getCatalogMeta().getCatalogProperties());
    return lanceCatalogFactory.create(
        catalogName, getMetastoreType(), paimonCatalogProperties, metaStore);
  }

  @Override
  public LanceNamespace originalCatalog() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    TableMetaStore metaStore = CatalogUtil.buildMetaStore(getCatalogMeta());
    Map<String, String> properties =
        factory.convertCatalogProperties(catalogName, getMetastoreType(), catalogProperties);
    LanceCatalog catalog = factory.create(catalogName, getMetastoreType(), properties, metaStore);
    return catalog.originalCatalog();
  }

  @Override
  public void setTableProperties(String db, String tableName, String key, String value) {
    try {
      // TODO
      originalCatalog();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeTableProperties(String db, String tableName, String key) {
    try {
      // TODO
      originalCatalog();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clean() {
    try {
      LanceNamespace catalog = originalCatalog();
      ListNamespacesRequest request = new ListNamespacesRequest();
      for (String dbName : catalog.listNamespaces(request).getNamespaces()) {
        try {
          DropNamespaceRequest dropNamespaceRequest =
              new DropNamespaceRequest().id(List.of(dbName));
          catalog.dropNamespace(dropNamespaceRequest);
          continue;
        } catch (Exception e) {
          // If drop database failed, drop all tables in this database. Because 'default' database
          // can not be
          // dropped in hive catalog.
        }
        ListTablesRequest listTablesRequest = new ListTablesRequest().id(List.of(dbName));
        for (String tableName : catalog.listTables(listTablesRequest).getTables()) {
          DropTableRequest dropTableRequest = new DropTableRequest().id(List.of(dbName, tableName));
          catalog.dropTable(dropTableRequest);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createTable(String db, String tableName) throws Exception {
    try {
      LanceNamespace catalog = originalCatalog();
      CreateEmptyTableRequest createEmptyTableRequest =
          new CreateEmptyTableRequest().id(List.of(db, tableName));
      catalog.createEmptyTable(createEmptyTableRequest);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createDatabase(String database) throws Exception {
    try {
      LanceNamespace catalog = originalCatalog();
      CreateNamespaceRequest request = new CreateNamespaceRequest().id(List.of(database));
      catalog.createNamespace(request);
    } catch (Exception e) {
      throw new AlreadyExistsException(e);
    }
  }

  public static LanceDirectoryCatalogTestHelper defaultHelper() {
    return new LanceDirectoryCatalogTestHelper("test_paimon_catalog", new HashMap<>());
  }
}
