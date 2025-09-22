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
import com.lancedb.lance.Version;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.table.TableIdentifier;

import java.util.Map;

public class LanceTable implements AmoroTable<Dataset> {

  private final TableIdentifier tableIdentifier;

  private final Dataset table;

  public LanceTable(TableIdentifier tableIdentifier, Dataset table) {
    this.tableIdentifier = tableIdentifier;
    this.table = table;
  }

  @Override
  public TableIdentifier id() {
    return tableIdentifier;
  }

  @Override
  public TableFormat format() {
    return TableFormat.LANCE;
  }

  @Override
  public Map<String, String> properties() {
    return table.getConfig();
  }

  @Override
  public Dataset originalTable() {
    return table;
  }

  @Override
  public TableSnapshot currentSnapshot() {
    Version version = table.listVersions().get(0);
    return version == null ? null : new LanceSnapshot(version);
  }
}
