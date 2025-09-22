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
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.JsonArrowDataType;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.util.ArrowIpcUtil;
import org.apache.amoro.formats.AmoroCatalogTestHelper;
import org.apache.amoro.formats.TestAmoroCatalogBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestLanceAmoroCatalog extends TestAmoroCatalogBase {

  public TestLanceAmoroCatalog(AmoroCatalogTestHelper<?> amoroCatalogTestHelper) {
    super(amoroCatalogTestHelper);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[] {LanceDirectoryCatalogTestHelper.defaultHelper()};
  }

  @Override
  protected void createDatabase(String dbName) {
    try {
      LanceNamespace catalog = catalog();
      CreateNamespaceRequest request = new CreateNamespaceRequest().id(List.of(dbName));
      catalog.createNamespace(request);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void createTable(String dbName, String tableName, Map<String, String> properties) {
    try {
      LanceNamespace catalog = catalog();
      CreateTableRequest request = new CreateTableRequest().id(List.of(dbName, tableName));
      catalog.createTable(request, createTestArrowData());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<String> listDatabases() {
    try {
      LanceNamespace catalog = catalog();
      return new ArrayList<>(catalog.listNamespaces(new ListNamespacesRequest()).getNamespaces());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private LanceNamespace catalog() {
    return (LanceNamespace) originalCatalog;
  }

  private byte[] createTestArrowData() throws IOException {
    // Create a valid Arrow IPC stream with test schema
    JsonArrowSchema schema = createTestSchema();
    return ArrowIpcUtil.createEmptyArrowIpcStream(schema);
  }

  private JsonArrowSchema createTestSchema() {
    // Create a simple schema with id (int32) and name (string) fields
    JsonArrowDataType intType = new JsonArrowDataType();
    intType.setType("int32");

    JsonArrowDataType stringType = new JsonArrowDataType();
    stringType.setType("utf8");

    JsonArrowField idField = new JsonArrowField();
    idField.setName("id");
    idField.setType(intType);
    idField.setNullable(false);

    JsonArrowField nameField = new JsonArrowField();
    nameField.setName("name");
    nameField.setType(stringType);
    nameField.setNullable(true);

    List<JsonArrowField> fields = new ArrayList<>();
    fields.add(idField);
    fields.add(nameField);

    JsonArrowSchema schema = new JsonArrowSchema();
    schema.setFields(fields);
    return schema;
  }
}
