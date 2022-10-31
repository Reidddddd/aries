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

package org.apache.aries.action;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class AlterBloomFilter extends AlterBase {

  private BloomType bloom;

  public AlterBloomFilter() {}

  protected void alter(TableName table) throws Exception {
    switch (bloom) {
      case    ROW: bloom = RANDOM.nextBoolean() ? BloomType.ROWCOL : BloomType.NONE; break;
      case ROWCOL: bloom = RANDOM.nextBoolean() ? BloomType.ROW    : BloomType.NONE; break;
      case   NONE: bloom = RANDOM.nextBoolean() ? BloomType.ROWCOL : BloomType.ROW;  break;
    }
    descriptor.setBloomFilterType(bloom);
    admin.modifyColumn(table, descriptor);
  }

  protected void preAlter(TableName table) throws Exception {
    super.preAlter(table);
    descriptor = admin.getTableDescriptor(table).getColumnFamilies()[0];
         bloom = descriptor.getBloomFilterType();
    LOG.info("Current bloom filter of " + table + " is " + bloom);
  }

  protected void postAlter(TableName table) throws Exception {
    super.postAlter(table);
    LOG.info("Finish altering bloom filter of " + table + " to " + bloom + " in " + getDuration() + " seconds");
  }

}
