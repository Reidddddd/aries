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

import org.apache.aries.common.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;

public class AlterBlockSize extends AlterBase {

  private final static int TEN_MB = (int) (Constants.ONE_MB * 10);

  private int block_size;

  public AlterBlockSize() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    block_size = RANDOM.nextInt(TEN_MB);
    if (block_size == 0) {
      block_size += Constants.ONE_KB;
    }
    family.setBlocksize(block_size);
    admin.modifyColumn(table, family);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    block_size = family.getBlocksize();
    LOG.info(preLogMessage("block size", table, family, block_size));
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info(postLogMessage("block size", table, family, block_size));
  }

}
