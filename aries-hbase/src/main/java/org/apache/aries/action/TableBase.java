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

import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Random;

public abstract class TableBase extends Action {

  public final static String TABLE_NAME = "table_base_action.table.name";
  public final static String   X_ROUNDS = "table_base_action.act.rounds";
  public final static String SLEEP_SECS = "table_base_action.sleep.between.rounds";

  protected final Random RANDOM = new Random();

  protected boolean random_pick = false;
  protected int x_rounds;
  protected int sleep_secs;
  protected TableName table;
  protected Admin admin;
  protected long start_time;
  protected long duration;

  public TableBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
                admin = connection.getAdmin();
             x_rounds = configuration.getInt("cr." + X_ROUNDS, 1);
           sleep_secs = configuration.getInt("cr." + SLEEP_SECS, 1);
    String table_name = configuration.get("cr." + TABLE_NAME);
    if (table_name == null || table_name.isEmpty()) random_pick = true;
    else table = TableName.valueOf(table_name);

  }

  @Override
  public final Integer call() throws Exception {
    boolean first_round = true;
    try {
      for (int i = 0; i < x_rounds; i++) {
        if (first_round) first_round = false;
        else Thread.sleep(ToyUtils.getTimeoutInMilliSeconds(sleep_secs));

        TableName picked;
        if (random_pick) {
          TableName[] tables = admin.listTableNames();
          picked = tables[RANDOM.nextInt(tables.length)];
        } else {
          picked = table;
        }
        prePerform(picked);
        perform(picked);
        postPerform(picked);
      }
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return 1;
    }
    return 0;
  }

  protected abstract void perform(TableName table) throws Exception;

  protected void prePerform(TableName table) throws Exception {
    start_time = System.currentTimeMillis();
  }

  protected void postPerform(TableName table) throws Exception {
    duration = System.currentTimeMillis() - start_time;
  }

  protected long getDuration() {
    return ToyUtils.getTimeoutInSeconds(duration);
  }
}
