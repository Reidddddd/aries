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

import org.apache.aries.ConfigurationFactory;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.workload.common.BaseHandler;
import org.apache.aries.workload.common.KEY_PREFIX;
import org.apache.aries.common.Parameter;
import org.apache.aries.workload.common.VALUE_KIND;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

public abstract class HandlerFactory {

  protected final Configuration hbase_conf;

  public HandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    hbase_conf = ConfigurationFactory.createHBaseConfiguration(configuration);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(BaseHandler.FAMILY))            hbase_conf.set(BaseHandler.FAMILY, (String) parameter.value());
      if (parameter.key().contains(BaseHandler.KEY_KIND))          hbase_conf.setEnum(BaseHandler.KEY_KIND, (KEY_PREFIX) parameter.value());
      if (parameter.key().contains(BaseHandler.KEY_LENGTH))        hbase_conf.setInt(BaseHandler.KEY_LENGTH, (Integer) parameter.value());
      if (parameter.key().contains(BaseHandler.VALUE_KINE))        hbase_conf.setEnum(BaseHandler.VALUE_KINE, (VALUE_KIND) parameter.value());
      if (parameter.key().contains(BaseHandler.RECORDS_NUM))       hbase_conf.setInt(BaseHandler.RECORDS_NUM, (Integer) parameter.value());
      if (parameter.key().contains(BaseHandler.SHARED_CONNECTION)) hbase_conf.setBoolean(BaseHandler.SHARED_CONNECTION, (Boolean) parameter.value());
    }
  }

  public abstract BaseHandler createHandler(TableName table) throws IOException;

}
