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

package org.apache.aries.workload.handler;

import com.codahale.metrics.Timer;
import org.apache.aries.workload.common.BaseHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutHandler extends BaseHandler {

  public static final String BUFFER_SIZE = "buffer_size";
  public static final String RANDOM_OPS = "random_key";
  public static final String VALUE_SIZE = "value_size_in_bytes";

  public PutHandler(Configuration conf, TableName table) throws IOException {
    super(conf, table);
  }

  @Override
  public void run() {
    BufferedMutator mutator;
    BufferedMutatorParams param = new BufferedMutatorParams(getTable());
    param.writeBufferSize(hbase_conf.getLong(BUFFER_SIZE, 0L));
    try {
      mutator = connection.getBufferedMutator(param);
      while (!isInterrupted() && sequence.get() <= records_num) {
        try (final Timer.Context context = LATENCY.time()) {
          String k = getKey(key_kind, key_length, hbase_conf.getBoolean(RANDOM_OPS, true));
          byte[] value = getValue(value_kind, k, hbase_conf.getInt(VALUE_SIZE, 0));
          Put put = new Put(Bytes.toBytes(k));
          put.addColumn(
              Bytes.toBytes(family),
              Bytes.toBytes("q"),
              value
          );
          mutator.mutate(put);
        }
      }
      mutator.flush();
      mutator.close();
    } catch (Exception e) {
      LOG.warning("Error occured!");
      e.printStackTrace();
    } finally {
      if (callback != null) {
        callback.onFinished();
      }
    }
  }

}

