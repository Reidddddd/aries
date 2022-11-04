/*
 * Copyright (c) 2019 R.C
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries.chaos;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;

public class AlterPrefetchBlocksOnOpen extends AlterBase {

  private boolean prefetch;

  public AlterPrefetchBlocksOnOpen() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    family.setPrefetchBlocksOnOpen(!prefetch);
    admin.modifyColumn(table, family);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    prefetch = family.isPrefetchBlocksOnOpen();
    LOG.info(preLogMessage("prefetch block", table, family, prefetch));
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info(postLogMessage("prefetch block", table, family, !prefetch));
  }

}
