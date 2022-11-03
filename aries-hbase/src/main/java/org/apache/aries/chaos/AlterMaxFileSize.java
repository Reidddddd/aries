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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class AlterMaxFileSize extends AlterBase {

  private final static long AT_LEAST_SIZE = Constants.ONE_MB * 10;

  private long file_size;
  private HTableDescriptor descriptor;

  public AlterMaxFileSize() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    file_size = RANDOM.nextLong();
    if (file_size < AT_LEAST_SIZE) {
      file_size = AT_LEAST_SIZE;
    }
    descriptor.setMaxFileSize(file_size);
    admin.modifyTable(table, descriptor);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    descriptor = admin.getTableDescriptor(table);
    file_size = descriptor.getMaxFileSize();
    LOG.info(preLogMessage("max file size", table, family, file_size));
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info(postLogMessage("block size", table, family, file_size));
  }

}
