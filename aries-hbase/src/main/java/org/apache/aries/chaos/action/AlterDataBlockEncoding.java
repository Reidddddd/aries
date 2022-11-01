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

package org.apache.aries.chaos.action;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

public class AlterDataBlockEncoding extends AlterBase {

  private DataBlockEncoding encoding;

  public AlterDataBlockEncoding() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    int index = RANDOM.nextInt(5);
    while (true) {
      DataBlockEncoding new_encode;
      switch (index) {
         case 0: new_encode = DataBlockEncoding.NONE;         break;
         case 1: new_encode = DataBlockEncoding.DIFF;         break;
         case 2: new_encode = DataBlockEncoding.FAST_DIFF;    break;
         case 3: new_encode = DataBlockEncoding.ROW_INDEX_V1; break;
         case 4: new_encode = DataBlockEncoding.PREFIX;       break;
        default: new_encode = DataBlockEncoding.NONE;         break;
      }
      if (new_encode == encoding) {
        continue;
      }
      encoding = new_encode;
      break;
    }
    family.setDataBlockEncoding(encoding);
    admin.modifyColumn(table, family);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    encoding = family.getDataBlockEncoding();
    LOG.info("Current data block encoding of " + table + " is " + encoding);
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info("Finish altering data block encoding of " + table + " to " + encoding + " in " + getDuration() + " seconds");
  }

}
