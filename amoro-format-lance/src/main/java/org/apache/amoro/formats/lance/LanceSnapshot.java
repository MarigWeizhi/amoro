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

import com.lancedb.lance.Version;
import org.apache.amoro.TableSnapshot;

public class LanceSnapshot implements TableSnapshot {

  private final Version version;

  public LanceSnapshot(Version version) {
    this.version = version;
  }

  @Override
  public long watermark() {
    return -1;
  }

  @Override
  public long commitTime() {
    return version.getDataTime().toInstant().toEpochMilli();
  }

  @Override
  public String id() {
    return version.getId() + "";
  }

  @Override
  public String operation() {
    // TODO
    return "";
  }

  @Override
  public long schemaId() {
    // TODO
    return -1;
  }
}
