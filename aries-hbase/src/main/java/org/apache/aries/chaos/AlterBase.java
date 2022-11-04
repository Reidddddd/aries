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

import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;

public abstract class AlterBase extends Action {

  public final static String  TABLE_NAME = "alter_base_action.table.name";
  public final static String FAMILY_NAME = "alter_base_action.family.name";

  protected final Random RANDOM = new Random();
  protected final byte[] EMPTY = Bytes.toBytes("");

  protected boolean random_pick = false;
  protected TableName table;
  protected Admin admin;
  protected long start_time;
  protected long duration;

  private byte[] family;

  public AlterBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
                admin = connection.getAdmin();
    String table_name = configuration.get("cr." + TABLE_NAME);
    if (table_name == null || table_name.isEmpty()) random_pick = true;
    else table = TableName.valueOf(table_name);

    String family_name = configuration.get("cr." + FAMILY_NAME);
    family = (family_name == null || family_name.isEmpty()) ? EMPTY : Bytes.toBytes(family_name);
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

      HColumnDescriptor descriptor = Bytes.equals(family, EMPTY) ?
          admin.getTableDescriptor(picked).getColumnFamilies()[0] :
          admin.getTableDescriptor(picked).getFamily(family);
      if (descriptor == null) {
        // it means null == admin.getTableDescriptor(table).getFamily(family);
        // means use configed wrong family name
        descriptor = admin.getTableDescriptor(picked).getColumnFamilies()[0];
      }

      preAlter(picked, descriptor);
      alter(picked, descriptor);
      postAlter(picked, descriptor);
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return RETURN_CODE.FAILURE.code();
    }
    return RETURN_CODE.SUCCESS.code();
  }

  protected abstract void alter(TableName table, HColumnDescriptor family) throws Exception;

  protected void preAlter(TableName table, HColumnDescriptor family) throws Exception {
    start_time = System.currentTimeMillis();
  }

  protected void postAlter(TableName table, HColumnDescriptor family) throws Exception {
    duration = System.currentTimeMillis() - start_time;
  }

  protected long getDuration() {
    return ToyUtils.getTimeoutInSeconds(duration);
  }

  protected String preLogMessage(String what, TableName table, HColumnDescriptor family, Object any) {
    return String.format("Current %s of %s@%s is %s", what, table, family.getNameAsString(), any);
  }

  protected String postLogMessage(String what, TableName table, HColumnDescriptor family, Object any) {
    return String.format("Finish altering %s of %s@%s to %s in %d seconds", what, table, family.getNameAsString(), any, getDuration());
  }

}
