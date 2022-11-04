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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;

public class AlterDurablity extends AlterBase {

  private Durability durability;
  private HTableDescriptor descriptor;

  public AlterDurablity() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    while (true) {
      int index = RANDOM.nextInt(4);
      Durability new_durability;
      switch (index) {
         case 0: new_durability = Durability.ASYNC_WAL;   break;
         case 1: new_durability = Durability.FSYNC_WAL;   break;
         case 2: new_durability = Durability.SYNC_WAL;    break;
         case 3: new_durability = Durability.SKIP_WAL;    break;
        default: new_durability = Durability.USE_DEFAULT; break;
      }
      if (new_durability == durability) {
        continue;
      }
      durability = new_durability;
      break;
    }
    descriptor.setDurability(durability);
    admin.modifyTable(table, descriptor);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    descriptor = admin.getTableDescriptor(table);
    durability = descriptor.getDurability();
    LOG.info(preLogMessage("durability", table, family, durability));
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info(postLogMessage("durability", table, family, durability));
  }

}
