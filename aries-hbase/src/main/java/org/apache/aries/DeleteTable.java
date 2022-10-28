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
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class DeleteTable extends AbstractHBaseToy {

  private final Parameter<String[]> tables =
      StringArrayParameter.newBuilder("dt.table_name").setRequired()
                          .setDescription("Tables's names to be dropped, delimited by ','. Pattern is supported, by prefixing '#'")
                          .addConstraint(v -> v.length > 0).opt();

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(tables);
  }

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
  }

  private Admin admin;

  @Override protected int haveFun() throws Exception {
    List<TableName> pending = new ArrayList<>();
    for (String table_or_pattern : tables.value()) {
      if (table_or_pattern.startsWith("#")) {
        pending.addAll(Arrays.asList(admin.listTableNames(Pattern.compile(table_or_pattern.substring(1)))));
      } else {
        pending.add(TableName.valueOf(table_or_pattern));
      }
    }

    for (TableName name : pending) {
      if (admin.tableExists(name)) {
        try {
          admin.disableTable(name);
        } catch (TableNotEnabledException tne) {
          LOG.warning(name + " is disabled already");
        }
        admin.deleteTable(name);
      }
    }
    return RETURN_CODE.SUCCESS.code();
  }

  @Override protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  @Override
  protected void exampleConfiguration() {
    example(tables.key(), "what:foo,hello:world,#test:.*");
  }

  @Override protected String getParameterPrefix() {
    return "dt";
  }
}
