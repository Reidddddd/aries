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
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class AlterCompression extends AlterBase {

  private Algorithm compression;

  public AlterCompression() {}

  protected void alter(TableName table) throws Exception {
    int index = RANDOM.nextInt(7);
    while (true) {
      Algorithm new_algo;
      switch (index) {
         case 0: new_algo = Algorithm.NONE;   break;
         case 1: new_algo = Algorithm.ZSTD;   break;
         case 2: new_algo = Algorithm.BZIP2;  break;
         case 3: new_algo = Algorithm.GZ;     break;
         case 4: new_algo = Algorithm.LZ4;    break;
         case 5: new_algo = Algorithm.LZO;    break;
         case 6: new_algo = Algorithm.SNAPPY; break;
        default: new_algo = Algorithm.NONE;   break;
      }
      if (new_algo == compression) {
        continue;
      }
      compression = new_algo;
      break;
    }
    descriptor.setCompressionType(compression);
    admin.modifyColumn(table, descriptor);
  }

  protected void preAlter(TableName table) throws Exception {
    super.preAlter(table);
    descriptor = admin.getTableDescriptor(table).getColumnFamilies()[0];
    compression = descriptor.getCompression();
    LOG.info("Current compression of " + table + " is " + compression);
  }

  protected void postAlter(TableName table) throws Exception {
    super.postAlter(table);
    LOG.info("Finish altering compression of " + table + " to " + compression + " in " + getDuration() + " seconds");
  }

}
