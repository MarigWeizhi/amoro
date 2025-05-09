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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.sql.types.IntegerType

private[sql] object TruncateTransform {
  def unapply(expr: Expression): Option[(Int, FieldReference)] = expr match {
    case transform: Transform =>
      transform match {
        case NamedTransform("truncate", Seq(Ref(seq: Seq[String]), Lit(value: Int, IntegerType))) =>
          Some((value, FieldReference(seq)))
        case NamedTransform("truncate", Seq(Lit(value: Int, IntegerType), Ref(seq: Seq[String]))) =>
          Some((value, FieldReference(seq)))
        case _ =>
          None
      }
    case _ =>
      None
  }
}
