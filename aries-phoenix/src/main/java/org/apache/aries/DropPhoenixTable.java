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

import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringArrayParameter;

import java.sql.PreparedStatement;
import java.util.List;

@SuppressWarnings("rawtypes")
public class DropPhoenixTable extends AbstractPhoenixToy {
  private final Parameter<String[]> tables =
      StringArrayParameter.newBuilder("dpt.tables").setRequired()
                          .setDescription("Phoenix tables to be dropped").addConstraint(v -> v.length > 0)
                          .opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(tables);
  }

  @Override
  protected int haveFun() throws Exception {
    PreparedStatement ps;
    for (String table : tables.value()) {
      ps = connection.prepareStatement(String.format("drop table if exists %s", table));
      ps.execute();
      LOG.info("Table " + table + " is dropped");
    }
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  protected void exampleConfiguration() {
    example(tables.key(), "PHOENIX.TABLE1,PHOENIX.TABLE2,PHOENIX.TABLE3");
  }

  @Override protected String getParameterPrefix() {
    return "dpt";
  }

}
