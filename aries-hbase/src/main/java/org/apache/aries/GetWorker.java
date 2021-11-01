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

import org.apache.aries.common.BaseHandler;
import org.apache.aries.common.BaseWorker;
import org.apache.aries.common.VALUE_KIND;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class GetWorker extends BaseWorker {

  private final AtomicLong effective_read = new AtomicLong(0);
  private final AtomicLong empty_read = new AtomicLong(0);
  private final AtomicLong correct_read = new AtomicLong(0);
  private final AtomicLong wrong_read = new AtomicLong(0);

  @Override
  protected BaseHandler createHandler(ToyConfiguration configuration) throws IOException {
    return new GetHandler(configuration);
  }

  @Override
  protected int haveFun() throws Exception {
    super.haveFun();
    LOG.info("Effective get " + effective_read.get() + " rows in " + running_time.value() + " seconds.");
    LOG.info("Empty get " + empty_read.get() + " in " + running_time.value() + " seconds.");
    if (kind == VALUE_KIND.FIXED) {
      LOG.info("Correct get " + correct_read.get() + " rows.");
      LOG.info("Wrong get " + wrong_read.get() + " rows.");
    }
    LOG.info("Existing.");
    return 0;
  }

  @Override protected String getParameterPrefix() {
    return "gw";
  }

  class GetHandler extends BaseHandler {

    long empty_get;
    long effective_get;
    long correct_get;
    long wrong_get;

    GetHandler(ToyConfiguration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(table);
        while (running) {
          String key = getKey(key_prefix, key_length.value());
          Get get = new Get(Bytes.toBytes(key));
          get.addColumn(
              Bytes.toBytes(family.value()),
              Bytes.toBytes("q")
          );
          Result result = target_table.get(get);
          if (result.isEmpty()) {
            empty_get++;
          } else {
            effective_get++;
            byte[] value = result.getValue(Bytes.toBytes(family.value()), Bytes.toBytes("q"));
            if (kind == VALUE_KIND.FIXED) {
              if (verifiedResult(kind, key, value)) {
                correct_get++;
              } else {
                wrong_get++;
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warning("Error occured " + e.getMessage());
      } finally {
        effective_read.addAndGet(effective_get);
        empty_read.addAndGet(empty_get);
        correct_read.addAndGet(correct_get);
        wrong_read.addAndGet(empty_get);
      }
    }

  }

}
