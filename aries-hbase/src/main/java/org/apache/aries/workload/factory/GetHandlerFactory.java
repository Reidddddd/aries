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

package org.apache.aries.workload.factory;

import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.Parameter;
import org.apache.aries.workload.common.BaseHandler;
import org.apache.aries.workload.handler.GetHandler;
import org.apache.hadoop.hbase.TableName;
import java.io.IOException;
import java.util.List;

public class GetHandlerFactory extends HandlerFactory {

  public GetHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    super(configuration, parameters);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(GetHandler.RANDOM_OPS))          hbase_conf.setBoolean(GetHandler.RANDOM_OPS, (Boolean) parameter.value());
      if (parameter.key().contains(GetHandler.RESULT_VERIFICATION)) hbase_conf.setBoolean(GetHandler.RESULT_VERIFICATION, (Boolean) parameter.value());
    }
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    return new GetHandler(hbase_conf, table);
  }

}
