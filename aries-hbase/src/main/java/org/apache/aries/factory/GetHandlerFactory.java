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

package org.apache.aries.factory;

import com.codahale.metrics.Timer;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.VALUE_KIND;
import org.apache.aries.handler.ReadHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class GetHandlerFactory extends HandlerFactory {

  public GetHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    super(configuration, parameters);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(GetHandler.RANDOM_OPS))  hbase_conf.setBoolean(GetHandler.RANDOM_OPS, (Boolean) parameter.value());
      if (parameter.key().contains(GetHandler.RESULT_VERIFICATION))  hbase_conf.setBoolean(GetHandler.RESULT_VERIFICATION, (Boolean) parameter.value());
    }
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    return new GetHandler(hbase_conf, table);
  }

  public static class GetHandler extends ReadHandler {

    public static final String RANDOM_OPS = "random_key";

    public static final String RESULT_VERIFICATION = "result_verification";

    GetHandler(Configuration conf, TableName table) throws IOException {
      super(conf, table);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(getTable());
        while (!isInterrupted()) {
          try (final Timer.Context context = LATENCY.time()) {
            String key = getKey(key_kind, key_length, hbase_conf.getBoolean(RANDOM_OPS, true));

            Get get = new Get(Bytes.toBytes(key));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes("q"));

            Result result = target_table.get(get);
            if (result.isEmpty()) {
              EMPTY_VALUE.inc();
              continue;
            }

            byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes("q"));
            if (hbase_conf.getBoolean(RESULT_VERIFICATION, false)) {
              if (value_kind == VALUE_KIND.FIXED) {
                if (!verifiedResult(value_kind, key, value)) {
                  WRONG_VALUE.inc();
                }
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warning("Error occured " + e.getMessage());
      }
    }

  }

}
