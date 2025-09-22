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
import org.apache.amoro.formats.AmoroCatalogTestHelper;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.TestServerTableDescriptor;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestLanceServerTableDescriptor extends TestServerTableDescriptor {

  public TestLanceServerTableDescriptor(AmoroCatalogTestHelper<?> amoroCatalogTestHelper) {
    super(amoroCatalogTestHelper);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[] {
      LanceDirectoryCatalogTestHelper.defaultHelper(), LanceHiveCatalogTestHelper.defaultHelper()
    };
  }

  @Override
  protected void tableOperationsAddColumns() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsRenameColumns() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnType() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnComment() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnRequired() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsDropColumn() {
    try {
      // TODO
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FormatTableDescriptor getTableDescriptor() {
    return new LanceTableDescriptor();
  }

  private LanceNamespace getCatalog() {
    return (LanceNamespace) getOriginalCatalog();
  }
}
