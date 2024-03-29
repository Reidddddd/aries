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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

public class AlterTimeToLive extends AlterBase {

  private int time_to_live;

  public AlterTimeToLive() {}

  protected void alter(TableName table, HColumnDescriptor family) throws Exception {
    time_to_live = RANDOM.nextInt(HConstants.FOREVER);
    if (time_to_live == 0) {
      time_to_live = HConstants.FOREVER;
    }
    family.setTimeToLive(time_to_live);
    admin.modifyColumn(table, family);
  }

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.preAlter(table, family);
    time_to_live = family.getTimeToLive();
    LOG.info(preLogMessage("time to live", table, family, time_to_live));
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    super.postAlter(table, family);
    LOG.info(postLogMessage("time to live", table, family, time_to_live));
  }

}
