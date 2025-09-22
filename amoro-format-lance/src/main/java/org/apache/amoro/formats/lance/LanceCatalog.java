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

import com.lancedb.lance.Dataset;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.ListNamespacesRequest;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import com.lancedb.lance.namespace.model.NamespaceExistsRequest;
import com.lancedb.lance.namespace.model.TableExistsRequest;
import org.apache.amoro.AlreadyExistsException;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.DatabaseNotEmptyException;
import org.apache.amoro.FormatCatalog;
import org.apache.amoro.NoSuchDatabaseException;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.table.TableIdentifier;

import java.util.ArrayList;
import java.util.List;

public class LanceCatalog implements FormatCatalog {

  private final LanceNamespace catalog;

  private final String name;

  public LanceCatalog(LanceNamespace catalog, String name) {
    this.catalog = catalog;
    this.name = name;
  }

  @Override
  public List<String> listDatabases() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    return new ArrayList<>(catalog.listNamespaces(request).getNamespaces());
  }

  @Override
  public boolean databaseExists(String database) {
    try {
      NamespaceExistsRequest request = new NamespaceExistsRequest().id(List.of(database));
      catalog.namespaceExists(request);
      return true;
    } catch (LanceNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean tableExists(String database, String table) {
    try {
      TableExistsRequest request = new TableExistsRequest().id(List.of(database, table));
      catalog.tableExists(request);
      return true;
    } catch (LanceNamespaceException e) {
      return false;
    }
  }

  @Override
  public void createDatabase(String database) {
    try {
      CreateNamespaceRequest request = new CreateNamespaceRequest().id(List.of(database));
      catalog.createNamespace(request);
    } catch (Exception e) {
      throw new AlreadyExistsException(e);
    }
  }

  @Override
  public void dropDatabase(String database) {
    try {
      DropNamespaceRequest request = new DropNamespaceRequest().id(List.of(database));
      catalog.dropNamespace(request);
    } catch (Exception e) {
      throw new DatabaseNotEmptyException(e);
    }
  }

  @Override
  public AmoroTable<?> loadTable(String database, String table) {
    try {
      DescribeTableRequest request = new DescribeTableRequest().id(List.of(database, table));
      DescribeTableResponse response = catalog.describeTable(request);
      return new LanceTable(
          TableIdentifier.of(name, database, table), Dataset.open(response.getLocation()));
    } catch (Exception e) {
      throw new NoSuchTableException(e);
    }
  }

  @Override
  public List<String> listTables(String database) {
    try {
      ListTablesRequest request = new ListTablesRequest();
      ListTablesResponse listTablesResponse = catalog.listTables(request);
      return new ArrayList<>(listTablesResponse.getTables());
    } catch (Exception e) {
      throw new NoSuchDatabaseException(e);
    }
  }

  @Override
  public boolean dropTable(String database, String table, boolean purge) {
    try {
      DropTableRequest request = new DropTableRequest();
      request.id(List.of(database, table));
      catalog.dropTable(request);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public LanceNamespace originalCatalog() {
    return catalog;
  }
}
