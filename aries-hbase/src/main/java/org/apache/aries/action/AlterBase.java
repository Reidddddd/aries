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

import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Random;

public abstract class AlterBase extends Action {

  public final static String TABLE_NAME = "alter_base_action.table.name";

  protected final Random RANDOM = new Random();

  protected boolean random_pick = false;
  protected TableName table;
  protected Admin admin;
  protected long start_time;
  protected long duration;
  protected HColumnDescriptor descriptor;

  public AlterBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
                admin = connection.getAdmin();
    String table_name = configuration.get("cr." + TABLE_NAME);
    if (table_name == null || table_name.isEmpty()) random_pick = true;
    else table = TableName.valueOf(table_name);

  }

  @Override
  public final Integer call() throws Exception {
    try {
      TableName picked;
      if (random_pick) {
        TableName[] tables = admin.listTableNames();
        picked = tables[RANDOM.nextInt(tables.length)];
      } else {
        picked = table;
      }

      preAlter(picked);
      alter(picked);
      postAlter(picked);
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return RETURN_CODE.FAILURE.code();
    }
    return RETURN_CODE.SUCCESS.code();
  }

  protected abstract void alter(TableName table) throws Exception;

  protected void preAlter(TableName table) throws Exception {
    start_time = System.currentTimeMillis();
  }

  protected void postAlter(TableName table) throws Exception {
    duration = System.currentTimeMillis() - start_time;
  }

  protected long getDuration() {
    return ToyUtils.getTimeoutInSeconds(duration);
  }

}
