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
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.KEY_PREFIX;
import org.apache.aries.common.VALUE_KIND;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.aries.common.Constants;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PutWorker extends AbstractHBaseToy {

  private final Parameter<Integer> num_connections =
      IntParameter.newBuilder("pw.num_connections").setRequired()
                  .setDescription("Number of connections used for put")
                  .addConstraint(v -> v > 0).opt();
  private final Parameter<String> table_name =
      StringParameter.newBuilder("pw.target_table").setRequired()
                     .setDescription("A table that data will be put in").opt();
  private final Parameter<String> family =
      StringParameter.newBuilder("pw.target_family")
                     .setDescription("A family that belongs to the target_table, and wanted to be put in data")
                     .setRequired().opt();
  private final Parameter<Long> buffer_size =
      LongParameter.newBuilder("pw.buffer_size").setDefaultValue(Constants.ONE_MB)
                   .setDescription("Buffer size in bytes for batch put").opt();
  private final Parameter<Integer> running_time =
      IntParameter.newBuilder("pw.running_time").setDescription("How long this application run (in seconds").opt();
  private final Parameter<Enum> value_kind =
      EnumParameter.newBuilder("pw.value_kind", VALUE_KIND.FIXED, VALUE_KIND.class)
                   .setDescription("Value is fixed or random generated").opt();
  private final Parameter<Integer> key_length =
      IntParameter.newBuilder("pw.key_length").setDefaultValue(Constants.DEFAULT_KEY_LENGTH_PW)
                  .setDescription("The length of the generated key in bytes.").opt();
  private final Parameter<Enum> key_kind =
      EnumParameter.newBuilder("pw.key_kind", KEY_PREFIX.NONE, KEY_PREFIX.class)
                   .setDescription("Key prefix type: NONE, HEX, DEC.").opt();

  private Admin admin;
  private ExecutorService service;
  private volatile boolean running = true;
  private TableName table;
  private final Object mutex = new Object();
  private VALUE_KIND kind;
  private KEY_PREFIX key_prefix;
  private AtomicLong totalRows = new AtomicLong(0);

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(num_connections);
    requisites.add(table_name);
    requisites.add(family);
    requisites.add(buffer_size);
    requisites.add(running_time);
    requisites.add(value_kind);
    requisites.add(key_length);
    requisites.add(key_kind);
  }

  @Override
  protected void exampleConfiguration() {
    example(num_connections.key(), "3");
    example(table_name.key(), "table:for_put");
    example(family.key(), "f");
    example(buffer_size.key(), "1024");
    example(running_time.key(), "300");
    example(value_kind.key(), "FIXED");
    example(key_length.key(), "10");
    example(key_kind.key(), "NONE");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    table = TableName.valueOf(table_name.value());
    admin = connection.getAdmin();
    if (!admin.tableExists(table)) {
      throw new TableNotFoundException(table);
    }

    service = Executors.newFixedThreadPool(num_connections.value());
    BaseHandler[] workers = new PutHandler[num_connections.value()];
    for (int i = 0; i < num_connections.value(); i++) {
      workers[i] = new PutHandler(configuration);
      service.submit(workers[i]);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      synchronized (mutex) {
        mutex.notify();
      }
    }));

    if (!running_time.empty()) {
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            TimeUnit.SECONDS.sleep(running_time.value());
          } catch (InterruptedException e) {
            // Ignore
          } finally {
            synchronized (mutex) {
              mutex.notify();
            }
          }
        }
      }, 0);
    }

    kind = (VALUE_KIND) value_kind.value();
    key_prefix = (KEY_PREFIX) key_kind.value();
  }

  @Override
  protected int haveFun() throws Exception {
    synchronized (mutex) {
      mutex.wait();
      running = false;
    }
    service.awaitTermination(30, TimeUnit.SECONDS);
    LOG.info("Total wrote " + totalRows.get() + " rows in " + running_time.value() + " seconds.");
    LOG.info("Avg " + (double) (totalRows.get()) / running_time.value());
    LOG.info("Existing.");
    return 0;
  }

  @Override
  protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  @Override protected String getParameterPrefix() {
    return "pw";
  }

  class PutHandler extends BaseHandler {

    long numberOfRows;

    PutHandler(ToyConfiguration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      BufferedMutator mutator;
      BufferedMutatorParams param = new BufferedMutatorParams(table);
      param.writeBufferSize(buffer_size.value());
      try {
        mutator = connection.getBufferedMutator(param);
        while (running) {
          String k = getKey(key_prefix, key_length.value());
          byte[] value = getValue(kind, k);
          Put put = new Put(Bytes.toBytes(k));
          put.addColumn(
              Bytes.toBytes(family.value()),
              Bytes.toBytes("q"),
              value
          );
          mutator.mutate(put);
          numberOfRows++;
        }
        mutator.flush();
        mutator.close();
      } catch (IOException e) {
        LOG.warning("Error occured " + e.getMessage());
      } finally {
        totalRows.addAndGet(numberOfRows);
      }
    }

  }

}
