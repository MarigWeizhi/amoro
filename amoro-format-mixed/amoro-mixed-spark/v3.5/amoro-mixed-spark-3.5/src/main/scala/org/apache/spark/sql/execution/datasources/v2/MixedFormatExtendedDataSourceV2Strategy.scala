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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.amoro.spark.{SparkUnifiedCatalog, SparkUnifiedSessionCatalog}
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog, SparkSessionCatalog}
import org.apache.spark.sql.{AnalysisException, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedNamespace}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.views.{CreateIcebergView, DropIcebergView, ResolvedV2View, ShowIcebergViews}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog}
import org.apache.spark.sql.execution.{OrderAwareCoalesceExec, SparkPlan}

/**
 * refer apache iceberg project 1.6.x branch
 * spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/
 * datasources/v2/ExtendedDataSourceV2Strategy.scala
 * @param spark
 */
case class MixedFormatExtendedDataSourceV2Strategy(spark: SparkSession) extends Strategy
  with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c @ Call(procedure, args) =>
      val input = buildInternalRow(args)
      CallExec(c.output, procedure, input) :: Nil

    case AddPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform, name) =>
      AddPartitionFieldExec(catalog, ident, transform, name) :: Nil

    case CreateOrReplaceBranch(
    IcebergCatalogAndIdentifier(catalog, ident), branch, branchOptions, create, replace, ifNotExists) =>
      CreateOrReplaceBranchExec(catalog, ident, branch, branchOptions, create, replace, ifNotExists) :: Nil

    case CreateOrReplaceTag(
    IcebergCatalogAndIdentifier(catalog, ident), tag, tagOptions, create, replace, ifNotExists) =>
      CreateOrReplaceTagExec(catalog, ident, tag, tagOptions, create, replace, ifNotExists) :: Nil

    case DropBranch(IcebergCatalogAndIdentifier(catalog, ident), branch, ifExists) =>
      DropBranchExec(catalog, ident, branch, ifExists) :: Nil

    case DropTag(IcebergCatalogAndIdentifier(catalog, ident), tag, ifExists) =>
      DropTagExec(catalog, ident, tag, ifExists) :: Nil

    case DropPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform) =>
      DropPartitionFieldExec(catalog, ident, transform) :: Nil

    case ReplacePartitionField(IcebergCatalogAndIdentifier(catalog, ident), transformFrom, transformTo, name) =>
      ReplacePartitionFieldExec(catalog, ident, transformFrom, transformTo, name) :: Nil

    case SetIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      SetIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case DropIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      DropIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case SetWriteDistributionAndOrdering(
    IcebergCatalogAndIdentifier(catalog, ident), distributionMode, ordering) =>
      SetWriteDistributionAndOrderingExec(catalog, ident, distributionMode, ordering) :: Nil

    case OrderAwareCoalesce(numPartitions, coalescer, child) =>
      OrderAwareCoalesceExec(numPartitions, coalescer, planLater(child)) :: Nil

    case RenameTable(ResolvedV2View(oldCatalog: ViewCatalog, oldIdent), newName, isView@true) =>
      val newIdent = Spark3Util.catalogAndIdentifier(spark, newName.toList.asJava)
      if (oldCatalog.name != newIdent.catalog().name()) {
        throw new AnalysisException(
          s"Cannot move view between catalogs: from=${oldCatalog.name} and to=${newIdent.catalog().name()}")
      }
      RenameV2ViewExec(oldCatalog, oldIdent, newIdent.identifier()) :: Nil

    case DropIcebergView(ResolvedIdentifier(viewCatalog: ViewCatalog, ident), ifExists) =>
      DropV2ViewExec(viewCatalog, ident, ifExists) :: Nil

    case CreateIcebergView(ResolvedIdentifier(viewCatalog: ViewCatalog, ident), queryText, query,
    columnAliases, columnComments, queryColumnNames, comment, properties, allowExisting, replace, _) =>
      CreateV2ViewExec(
        catalog = viewCatalog,
        ident = ident,
        queryText = queryText,
        columnAliases = columnAliases,
        columnComments = columnComments,
        queryColumnNames = queryColumnNames,
        viewSchema = query.schema,
        comment = comment,
        properties = properties,
        allowExisting = allowExisting,
        replace = replace) :: Nil

    case DescribeRelation(ResolvedV2View(catalog, ident), _, isExtended, output) =>
      DescribeV2ViewExec(output, catalog.loadView(ident), isExtended) :: Nil

    case ShowTableProperties(ResolvedV2View(catalog, ident), propertyKey, output) =>
      ShowV2ViewPropertiesExec(output, catalog.loadView(ident), propertyKey) :: Nil

    case ShowIcebergViews(ResolvedNamespace(catalog: ViewCatalog, namespace), pattern, output) =>
      ShowV2ViewsExec(output, catalog, namespace, pattern) :: Nil

    case ShowCreateTable(ResolvedV2View(catalog, ident), _, output) =>
      ShowCreateV2ViewExec(output, catalog.loadView(ident)) :: Nil

    case SetViewProperties(ResolvedV2View(catalog, ident), properties) =>
      AlterV2ViewSetPropertiesExec(catalog, ident, properties) :: Nil

    case UnsetViewProperties(ResolvedV2View(catalog, ident), propertyKeys, ifExists) =>
      AlterV2ViewUnsetPropertiesExec(catalog, ident, propertyKeys, ifExists) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, r)
  }

  /**
   * support UnifiedSessionCatalog
   */
  private object IcebergCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: SparkCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkUnifiedCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkUnifiedSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case _ =>
          None
      }
    }
  }
}
