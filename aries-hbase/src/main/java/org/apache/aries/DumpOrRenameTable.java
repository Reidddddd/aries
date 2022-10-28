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

package org.apache.aries;

import org.apache.aries.common.RETURN_CODE;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DumpOrRenameTable extends AbstractHBaseToy {

  private Parameter<String[]> old_table_names =
      StringArrayParameter.newBuilder("drt.old_table_names").setRequired()
                          .setDescription("Old tables's names").opt();
  private Parameter<String[]> new_table_names =
      StringArrayParameter.newBuilder("drt.new_table_names").setRequired()
                          .setDescription("New table names if it is a renaming operation, otherwise these will be used as snapshot names").opt();
  private Parameter<Boolean> dump_only =
      BoolParameter.newBuilder("drt.dump_snapshot_only", true)
                   .setDescription("Dump snapshot only, cloning and renaming will not happen").opt();

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(old_table_names);
    requisites.add(new_table_names);
    requisites.add(dump_only);
  }

  @Override protected void midCheck() {
    if (old_table_names.value().length != new_table_names.value().length) {
      throw new RuntimeException("table mapping is not one to one");
    }
  }

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);

    String[] old_tables = old_table_names.value();
    String[] new_tables = new_table_names.value();
    for (int i = 0; i < old_tables.length; i++) {
      table_mapping.put(old_tables[i], new_tables[i]);
    }
  }

  private Admin admin;
  private Map<String, String> table_mapping = new HashMap<>();

  @Override protected int haveFun() throws Exception {
    admin = connection.getAdmin();
    table_mapping.forEach((old_name, new_name) -> {
      TableName name = TableName.valueOf(old_name);
      if (dump_only.value()) {
        try {
          admin.snapshot(new_name, name);
        } catch (Exception e) {
          LOG.warning("Failed to snapshot " + name + " with reason " + e.getMessage());
        }
      } else {
        try {
          admin.snapshot(name.getQualifierAsString(), name);
          admin.cloneSnapshot(name.getQualifierAsString(), TableName.valueOf(new_name), true);
        } catch (Exception e) {
          LOG.warning("Failed to rename " + name + " with reason " + e.getMessage());
        }
      }
    });
    return RETURN_CODE.SUCCESS.code();
  }

  @Override protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  @Override
  protected void exampleConfiguration() {
    example(old_table_names.key(), "a,alice:toy,bob:table");
    example(new_table_names.key(), "b,bob:toy,bob:alice");
    example(dump_only.key(), "true");
  }

  @Override protected String getParameterPrefix() {
    return "drt";
  }

}
