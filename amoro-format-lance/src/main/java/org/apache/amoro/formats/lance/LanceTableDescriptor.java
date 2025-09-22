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
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.descriptor.AMSColumnInfo;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableSummary;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.amoro.utils.CommonUtil;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Descriptor for Paimon format tables. */
public class LanceTableDescriptor implements FormatTableDescriptor {

  public static final String PAIMON_MAIN_BRANCH_NAME = "main";

  private ExecutorService executor;

  @Override
  public void withIoExecutor(ExecutorService ioExecutor) {
    this.executor = ioExecutor;
  }

  @Override
  public List<TableFormat> supportFormat() {
    return Lists.newArrayList(TableFormat.LANCE);
  }

  @Override
  public ServerTableMeta getTableDetail(AmoroTable<?> amoroTable) {
    Dataset table = getTable(amoroTable);

    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableIdentifier(amoroTable.id());
    serverTableMeta.setTableType(amoroTable.format().name());

    // schema
    Schema schema = table.getSchema();

    serverTableMeta.setSchema(
        schema.getFields().stream()
            .map(
                s ->
                    new AMSColumnInfo(
                        s.getName(), s.getType().getTypeID().name(), !s.isNullable(), ""))
            .collect(Collectors.toList()));

    // primary key
    serverTableMeta.setPkList(new ArrayList<>());

    // partition
    serverTableMeta.setPartitionColumnList(new ArrayList<>());

    // properties
    serverTableMeta.setProperties(table.getTableMetadata());

    Map<String, Object> baseMetric = new HashMap<>();
    // table summary
    TableSummary tableSummary;
    Version version = table.listVersions().get(0);
    if (version != null) {
      AmoroSnapshotsOfTable snapshotsOfTable = snapshotsOfTable();
      long fileSize = snapshotsOfTable.getOriginalFileSize();
      String totalSize = CommonUtil.byteToXB(fileSize);
      int fileCount = snapshotsOfTable.getFileCount();

      String averageFileSize = CommonUtil.byteToXB(fileCount == 0 ? 0 : fileSize / fileCount);

      tableSummary =
          new TableSummary(
              fileCount, totalSize, averageFileSize, snapshotsOfTable.getRecords(), "paimon");

      baseMetric.put("totalSize", totalSize);
      baseMetric.put("fileCount", fileCount);
      baseMetric.put("averageFileSize", averageFileSize);
      baseMetric.put("lastCommitTime", version.getDataTime().toInstant().toEpochMilli());
    } else {
      tableSummary = new TableSummary(0, "0", "0", 0, "paimon");

      baseMetric.put("totalSize", 0);
      baseMetric.put("fileCount", 0);
      baseMetric.put("averageFileSize", 0);
    }
    serverTableMeta.setTableSummary(tableSummary);
    serverTableMeta.setBaseMetrics(baseMetric);

    return serverTableMeta;
  }

  private AmoroSnapshotsOfTable snapshotsOfTable() {
    return new AmoroSnapshotsOfTable("0", 0, 0, 0, 0, "", "", null);
  }

  @Override
  public List<AmoroSnapshotsOfTable> getSnapshots(
      AmoroTable<?> amoroTable, String ref, OperationType operationType) {
    return Lists.newArrayList();
  }

  @Override
  public List<PartitionFileBaseInfo> getSnapshotDetail(
      AmoroTable<?> amoroTable, String snapshotId, String ref) {
    return Lists.newArrayList();
  }

  @Override
  public List<DDLInfo> getTableOperations(AmoroTable<?> amoroTable) {
    return Lists.newArrayList();
  }

  @Override
  public List<PartitionBaseInfo> getTablePartitions(AmoroTable<?> amoroTable) {
    return Lists.newArrayList();
  }

  @Override
  public List<PartitionFileBaseInfo> getTableFiles(
      AmoroTable<?> amoroTable, String partition, Integer specId) {
    return Lists.newArrayList();
  }

  @Override
  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      AmoroTable<?> amoroTable, String type, ProcessStatus status, int limit, int offset) {
    return Pair.of(Lists.newArrayList(), 0);
  }

  @Override
  public Map<String, String> getTableOptimizingTypes(AmoroTable<?> amoroTable) {
    Map<String, String> types = Maps.newHashMap();
    types.put("FULL", "full");
    types.put("MINOR", "MINOR");
    return types;
  }

  @Override
  public List<OptimizingTaskInfo> getOptimizingTaskInfos(
      AmoroTable<?> amoroTable, String processId) {
    throw new UnsupportedOperationException();
  }

  @NotNull
  private static Map<String, String> extractSummary(Map<String, String> summary, String... keys) {
    Set<String> keySet = new HashSet<>(Arrays.asList(keys));
    return summary.entrySet().stream()
        .filter(e -> keySet.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public List<TagOrBranchInfo> getTableTags(AmoroTable<?> amoroTable) {
    return Lists.newArrayList();
  }

  @Override
  public List<TagOrBranchInfo> getTableBranches(AmoroTable<?> amoroTable) {
    return Lists.newArrayList();
  }

  @Override
  public List<ConsumerInfo> getTableConsumerInfos(AmoroTable<?> amoroTable) {
    return Lists.newArrayList();
  }

  private Dataset getTable(AmoroTable<?> amoroTable) {
    return (Dataset) amoroTable.originalTable();
  }
}
