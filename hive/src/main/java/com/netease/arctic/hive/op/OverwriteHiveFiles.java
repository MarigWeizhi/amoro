package com.netease.arctic.hive.op;

import com.netease.arctic.hive.HMSClient;
import com.netease.arctic.hive.exceptions.CannotAlterHiveLocationException;
import com.netease.arctic.hive.table.UnkeyedHiveTable;
import com.netease.arctic.hive.utils.HivePartitionUtil;
import com.netease.arctic.op.UpdatePartitionProperties;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.FileUtil;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class OverwriteHiveFiles implements OverwriteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(OverwriteHiveFiles.class);

  private final Transaction transaction;
  private final boolean insideTransaction;
  private final OverwriteFiles delegate;
  private final UnkeyedHiveTable table;
  private final HMSClient hmsClient;
  private final HMSClient transactionClient;
  private final String db;
  private final String tableName;

  private final Table hiveTable;

  private Expression expr;
  private final List<DataFile> addFiles = Lists.newArrayList();
  private final List<DataFile> deleteFiles = Lists.newArrayList();

  private Map<StructLike, Partition> partitionToDelete = Maps.newHashMap();
  private Map<StructLike, Partition> partitionToCreate = Maps.newHashMap();
  private String unpartitionTableLocation;
  private long txId = -1;

  public OverwriteHiveFiles(Transaction transaction, boolean insideTransaction, UnkeyedHiveTable table,
      HMSClient hmsClient, HMSClient transactionClient) {
    this.transaction = transaction;
    this.insideTransaction = insideTransaction;
    this.delegate = transaction.newOverwrite();
    this.table = table;
    this.hmsClient = hmsClient;
    this.transactionClient = transactionClient;

    this.db = table.id().getDatabase();
    this.tableName = table.id().getTableName();
    try {
      this.hiveTable = hmsClient.run(c -> c.getTable(db, tableName));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    delegate.overwriteByRowFilter(expr);
    this.expr = expr;
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    delegate.addFile(file);
    String hiveLocationRoot = table.hiveLocation();
    String dataFileLocation = file.path().toString();
    if (dataFileLocation.toLowerCase().contains(hiveLocationRoot.toLowerCase())) {
      // only handle file in hive location
      this.addFiles.add(file);
    }
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    delegate.deleteFile(file);
    String hiveLocation = table.hiveLocation();
    String dataFileLocation = file.path().toString();
    if (dataFileLocation.toLowerCase().contains(hiveLocation.toLowerCase())) {
      // only handle file in hive location
      this.deleteFiles.add(file);
    }
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    delegate.validateAddedFilesMatchOverwriteFilter();
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long snapshotId) {
    delegate.validateFromSnapshot(snapshotId);
    return this;
  }

  @Override
  public OverwriteFiles caseSensitive(boolean caseSensitive) {
    delegate.caseSensitive(caseSensitive);
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingAppends(Expression conflictDetectionFilter) {
    delegate.validateNoConflictingAppends(conflictDetectionFilter);
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingAppends(Long readSnapshotId, Expression conflictDetectionFilter) {
    delegate.validateNoConflictingAppends(readSnapshotId, conflictDetectionFilter);
    return this;
  }

  @Override
  public OverwriteFiles set(String property, String value) {
    if ("txId".equals(property)) {
      this.txId = Long.parseLong(value);
    }

    delegate.set(property, value);
    return this;
  }

  @Override
  public OverwriteFiles deleteWith(Consumer<String> deleteFunc) {
    delegate.deleteWith(deleteFunc);
    return this;
  }

  @Override
  public OverwriteFiles stageOnly() {
    delegate.stageOnly();
    return this;
  }

  @Override
  public Snapshot apply() {
    return delegate.apply();
  }

  @Override
  public void commit() {
    if (table.spec().isUnpartitioned()) {
      generateUnpartitionTableLocation();
    } else {
      this.partitionToDelete = getDeletePartition();
      this.partitionToCreate = getCreatePartition(this.partitionToDelete);
    }

    delegate.commit();
    setHiveLocations();
    if (!insideTransaction) {
      transaction.commitTransaction();
    }

    if (table.spec().isUnpartitioned()) {
      commitNonPartitionedTable();
    } else {
      commitPartitionedTable();
    }
  }

  private void setHiveLocations() {
    UpdatePartitionProperties updatePartitionProperties = table.updatePartitionProperties(transaction);
    if (table.spec().isUnpartitioned()) {
      updatePartitionProperties.set(TablePropertyUtil.EMPTY_STRUCT,
          TableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION, unpartitionTableLocation);
    } else {
      partitionToDelete.forEach((partitionData, partition) -> {
        if (!partitionToCreate.containsKey(partitionData)) {
          updatePartitionProperties.remove(partitionData, TableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION);
        }
      });
      partitionToCreate.forEach((partitionData, partition) -> {
        updatePartitionProperties.set(partitionData, TableProperties.PARTITION_PROPERTIES_KEY_HIVE_LOCATION,
            partition.getSd().getLocation());
      });
    }
    updatePartitionProperties.commit();
  }

  @Override
  public Object updateEvent() {
    return delegate.updateEvent();
  }

  protected Map<StructLike, Partition> getCreatePartition(Map<StructLike, Partition> partitionToDelete) {
    if (this.addFiles.isEmpty()) {
      return Maps.newHashMap();
    }

    Map<String, String> partitionLocationMap = Maps.newHashMap();
    Map<String, List<DataFile>> partitionDataFileMap = Maps.newHashMap();
    Map<String, List<String>> partitionValueMap = Maps.newHashMap();

    Types.StructType partitionSchema = table.spec().partitionType();
    for (DataFile d : addFiles) {
      List<String> partitionValues = HivePartitionUtil.partitionValuesAsList(d.partition(), partitionSchema);
      String value = Joiner.on("/").join(partitionValues);
      String location = FileUtil.getFileDir(d.path().toString());
      partitionLocationMap.put(value, location);
      if (!partitionDataFileMap.containsKey(value)) {
        partitionDataFileMap.put(value, Lists.newArrayList());
      }
      partitionDataFileMap.get(value).add(d);
      partitionValueMap.put(value, partitionValues);
    }

    Map<StructLike, Partition> createPartitions = Maps.newHashMap();
    for (String val : partitionValueMap.keySet()) {
      List<String> values = partitionValueMap.get(val);
      String location = partitionLocationMap.get(val);
      List<DataFile> dataFiles = partitionDataFileMap.get(val);

      checkCreatePartitionDataFiles(dataFiles, location);
      Partition p = HivePartitionUtil.newPartition(hiveTable, values, location, dataFiles);
      createPartitions.put(dataFiles.get(0).partition(), p);
    }

    createPartitions = filterNewPartitionNonExists(createPartitions, partitionToDelete);
    return createPartitions;
  }

  protected Map<StructLike, Partition> getDeletePartition() {
    if (expr != null) {
      List<DataFile> deleteFilesByExpr = applyDeleteExpr();
      this.deleteFiles.addAll(deleteFilesByExpr);
    }

    Map<StructLike, Partition> deletePartitions = Maps.newHashMap();
    if (deleteFiles.isEmpty()) {
      return deletePartitions;
    }

    Types.StructType partitionSchema = table.spec().partitionType();

    Set<String> checkedPartitionValues = Sets.newHashSet();
    Set<Path> deleteFileLocations = Sets.newHashSet();

    for (DataFile dataFile : deleteFiles) {
      List<String> values = HivePartitionUtil.partitionValuesAsList(dataFile.partition(), partitionSchema);
      String pathValue = Joiner.on("/").join(values);
      deleteFileLocations.add(new Path(dataFile.path().toString()));
      if (checkedPartitionValues.contains(pathValue)) {
        continue;
      }
      try {
        Partition partition = hmsClient.run(c -> c.getPartition(db, tableName, values));
        deletePartitions.put(dataFile.partition(), partition);
      } catch (NoSuchObjectException e) {
        // pass do nothing
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      checkedPartitionValues.add(pathValue);
    }

    deletePartitions.values().forEach(p -> checkPartitionDelete(deleteFileLocations, p));
    return deletePartitions;
  }

  private void checkPartitionDelete(Set<Path> deleteFiles, Partition partition) {
    String partitionLocation = partition.getSd().getLocation();
    List<FileStatus> files = table.io().list(partitionLocation);
    for (FileStatus f : files) {
      Path filePath = f.getPath();
      if (!deleteFiles.contains(filePath)) {
        throw new CannotAlterHiveLocationException(
            "can't delete hive partition: " + partitionToString(partition) + ", file under partition is not deleted: " +
                filePath.toString());
      }
    }
  }

  /**
   * check all file with same partition key under same path
   */
  private void checkCreatePartitionDataFiles(List<DataFile> addFiles, String partitionLocation) {
    Path partitionPath = new Path(partitionLocation);
    for (DataFile df : addFiles) {
      String fileDir = FileUtil.getFileDir(df.path().toString());
      Path dirPath = new Path(fileDir);
      if (!partitionPath.equals(dirPath)) {
        throw new CannotAlterHiveLocationException(
            "can't create new hive location: " + partitionLocation + " for data file: " + df.path().toString() +
                " is not under partition location path"
        );
      }
    }
  }

  /**
   * filter partitionToCreate. make sure all partition non-exist in hive. or
   * 0. partition is able to delete.
   * 0.1 - not same location, allow to create
   * 0.2 - same location, can't create ( delete partition will not delete files )
   * 1. exists but location is same. skip
   * 2. exists but location is not same, throw {@link CannotAlterHiveLocationException}
   */
  private Map<StructLike, Partition> filterNewPartitionNonExists(
      Map<StructLike, Partition> partitionToCreate,
      Map<StructLike, Partition> partitionToDelete) {
    Map<StructLike, Partition> partitions = Maps.newHashMap();
    Map<String, Partition> deletePartitionValueMap = Maps.newHashMap();
    for (Partition p : partitionToDelete.values()) {
      String partValue = Joiner.on("/").join(p.getValues());
      deletePartitionValueMap.put(partValue, p);
    }

    for (Map.Entry<StructLike, Partition> entry : partitionToCreate.entrySet()) {
      String partValue = Joiner.on("/").join(entry.getValue().getValues());
      String location = entry.getValue().getSd().getLocation();
      Partition toDelete = deletePartitionValueMap.get(partValue);
      if (toDelete != null) {
        String deleteLocation = toDelete.getSd().getLocation();
        // if exists partition to delete with same value
        // make sure location is different
        if (isPathEquals(location, deleteLocation)) {
          throw new CannotAlterHiveLocationException("can't create new partition: " +
              partitionToString(entry.getValue()) + ", this " +
              "partition will be " +
              "delete and re-create with same location");
        } else {
          partitions.put(entry.getKey(), entry.getValue());
          continue;
        }
      }

      try {
        Partition partitionInHive = hmsClient.run(c -> c.getPartition(db, tableName, entry.getValue().getValues()));
        String locationInHive = partitionInHive.getSd().getLocation();
        if (isPathEquals(location, locationInHive)) {
          // exists same location, skip create operation
          continue;
        }
        throw new CannotAlterHiveLocationException("can't create new partition: " +
            partitionToString(entry.getValue()) +
            ", this partition exists in hive with different location: " + locationInHive);
      } catch (NoSuchObjectException e) {
        partitions.put(entry.getKey(), entry.getValue());
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return partitions;
  }

  private void commitPartitionedTable() {
    if (!partitionToDelete.isEmpty()) {
      for (Partition p : partitionToDelete.values()) {
        try {
          transactionClient.run(c -> {
            PartitionDropOptions options = PartitionDropOptions.instance()
                .deleteData(false)
                .ifExists(true)
                .purgeData(false)
                .returnResults(false);
            c.dropPartition(db, tableName, p.getValues(), options);
            return 0;
          });
        } catch (NoSuchObjectException e) {
          LOG.warn("try to delete hive partition {} but partition not exist.", p);
        } catch (TException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (!partitionToCreate.isEmpty()) {
      try {
        transactionClient.run(c -> c.add_partitions(partitionToCreate.values().stream().collect(Collectors.toList())));
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void generateUnpartitionTableLocation() {
    if (this.addFiles.isEmpty()) {
      unpartitionTableLocation = createEmptyLocationForHive();
    } else {
      unpartitionTableLocation = FileUtil.getFileDir(this.addFiles.get(0).path().toString());
    }
  }

  private void commitNonPartitionedTable() {

    final String finalLocation = unpartitionTableLocation;
    try {
      transactionClient.run(c -> {
        Table hiveTable = c.getTable(db, tableName);
        hiveTable.getSd().setLocation(finalLocation);
        c.alter_table(db, tableName, hiveTable);
        return 0;
      });
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private String createEmptyLocationForHive() {
    // create a new empty location for hive
    String newLocation = null;
    if (txId > 0) {
      newLocation = table.hiveLocation() + "/txId=" + txId;
    } else {
      newLocation = table.hiveLocation() + "/ts_" + System.currentTimeMillis();
    }
    OutputFile file = table.io().newOutputFile(newLocation + "/.keep");
    try {
      file.createOrOverwrite().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return newLocation;
  }

  protected List<DataFile> applyDeleteExpr() {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(expr).planFiles()) {
      return Lists.newArrayList(tasks).stream().map(FileScanTask::file).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isPathEquals(String pathA, String pathB) {
    Path path1 = new Path(pathA);
    Path path2 = new Path(pathB);
    return path1.equals(path2);
  }

  private String partitionToString(Partition p) {
    return "Partition(values: [" + Joiner.on("/").join(p.getValues()) +
        "], location: " + p.getSd().getLocation() + ")";
  }
}
