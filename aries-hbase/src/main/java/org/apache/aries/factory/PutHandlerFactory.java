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

import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class PutHandlerFactory extends HandlerFactory {

  public PutHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    super(configuration, parameters);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(PutHandler.BUFFER_SIZE)) hbase_conf.setLong(PutHandler.BUFFER_SIZE, (Long) parameter.value());
      if (parameter.key().contains(PutHandler.RANDOM_OPS))  hbase_conf.setBoolean(PutHandler.RANDOM_OPS, (Boolean) parameter.value());
    }
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    return new PutHandler(hbase_conf, table);
  }

  public static class PutHandler extends BaseHandler {

    public static final String BUFFER_SIZE = "buffer_size";
    public static final String RANDOM_OPS = "random_key";

    PutHandler(Configuration conf, TableName table) throws IOException {
      super(conf, table);
    }

    @Override
    public void run() {
      BufferedMutator mutator;
      BufferedMutatorParams param = new BufferedMutatorParams(getTable());
      param.writeBufferSize(hbase_conf.getLong(BUFFER_SIZE, 0L));
      try {
        mutator = connection.getBufferedMutator(param);
        while (!isInterrupted() && sequence.get() < records_num) {
          String k = getKey(key_kind, key_length, hbase_conf.getBoolean(RANDOM_OPS, true));
          byte[] value = getValue(value_kind, k);
          Put put = new Put(Bytes.toBytes(k));
          put.addColumn(
              Bytes.toBytes(family),
              Bytes.toBytes("q"),
              value
          );
          mutator.mutate(put);
        }
        mutator.flush();
        mutator.close();
      } catch (Exception e) {
        LOG.warning("Error occured " + e.getMessage());
      }
    }

  }

}
