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
import org.apache.aries.common.Constants;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.KEY_PREFIX;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.aries.common.VALUE_KIND;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ScanWorker extends AbstractHBaseToy {

  private final Parameter<Integer> num_connections =
      IntParameter.newBuilder("sw.num_connections").setRequired()
          .setDescription("Number of connections used for scan")
          .addConstraint(v -> v > 0).opt();
  private final Parameter<String> table_name =
      StringParameter.newBuilder("sw.target_table").setRequired()
          .setDescription("A table that data will be scanned").opt();
  private final Parameter<String> family =
      StringParameter.newBuilder("sw.target_family")
          .setDescription("A family that belongs to the target_table, and wanted to be read")
          .setRequired().opt();
  private final Parameter<Integer> running_time =
      IntParameter.newBuilder("sw.running_time").setDescription("How long this application run (in seconds").opt();
  private final Parameter<Enum> value_kind =
      EnumParameter.newBuilder("sw.value_kind", VALUE_KIND.FIXED, VALUE_KIND.class)
          .setDescription("After the value read, it will be used to verify the result").opt();
  private final Parameter<Integer> key_length =
      IntParameter.newBuilder("sw.key_length").setDefaultValue(Constants.DEFAULT_KEY_LENGTH_PW)
          .setDescription("The length of the generated key in bytes.").opt();
  private final Parameter<Enum> key_kind =
      EnumParameter.newBuilder("sw.key_kind", KEY_PREFIX.NONE, KEY_PREFIX.class)
          .setDescription("Key prefix type: NONE, HEX, DEC.").opt();

  private Admin admin;
  private ExecutorService service;
  private volatile boolean running = true;
  private TableName table;
  private final Object mutex = new Object();
  private VALUE_KIND kind;
  private KEY_PREFIX key_prefix;
  private AtomicLong effective_read = new AtomicLong(0);
  private AtomicLong empty_read = new AtomicLong(0);
  private AtomicLong correct_read = new AtomicLong(0);
  private AtomicLong wrong_read = new AtomicLong(0);

  @Override
  protected String getParameterPrefix() {
    return "sw";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(num_connections);
    requisites.add(table_name);
    requisites.add(family);
    requisites.add(running_time);
    requisites.add(value_kind);
    requisites.add(key_length);
    requisites.add(key_kind);
  }

  @Override
  protected void exampleConfiguration() {
    example(num_connections.key(), "3");
    example(table_name.key(), "table:for_scan");
    example(family.key(), "f");
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
    BaseHandler[] workers = new ScanHandler[num_connections.value()];
    for (int i = 0; i < num_connections.value(); i++) {
      workers[i] = new ScanHandler(configuration);
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
    LOG.info("Effective get " + effective_read.get() + " rows in " + running_time.value() + " seconds.");
    LOG.info("Empty get " + empty_read.get() + " in " + running_time.value() + " seconds.");
    if (kind == VALUE_KIND.FIXED) {
      LOG.info("Correct get " + correct_read.get() + " rows.");
      LOG.info("Wrong get " + wrong_read.get() + " rows.");
    }
    LOG.info("Existing.");
    return 0;
  }

  @Override
  protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  class ScanHandler extends BaseHandler {

    ScanHandler(ToyConfiguration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(table);
        while (running) {
          String key = getKey(key_prefix, key_length.value());
          Scan scan = new Scan();
          String k1 = getKey(key_prefix, key_length.value());
          String k2 = getKey(key_prefix, key_length.value());
          Pair<byte[], byte[]> boundaries = getBoundaries(k1, k2);
          scan.addFamily(Bytes.toBytes(family.value()));
          scan.withStartRow(boundaries.getFirst());
          scan.withStartRow(boundaries.getSecond());
          scan.setCacheBlocks(false);
          scan.addColumn(Bytes.toBytes(family.value()), Bytes.toBytes("q"));
          ResultScanner scanner = target_table.getScanner(scan);
          for (Result result = scanner.next(); result != null; result = scanner.next()) {
            if (result.isEmpty()) {
            } else {
              byte[] value = result.getValue(Bytes.toBytes(family.value()), Bytes.toBytes("q"));
              if (kind == VALUE_KIND.FIXED) {
                if (verifiedResult(kind, key, value)) {
                } else {
                }
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warning("Error occured " + e.getMessage());
      } finally {
      }
    }

    private Pair<byte[], byte[]> getBoundaries(String k1, String k2) {
      Pair<byte[], byte[]> boundaries = new Pair<>();
      int res = Bytes.compareTo(Bytes.toBytes(k1), Bytes.toBytes(k2));
      if (res < 0) {
        boundaries.setFirst(Bytes.toBytes(k1));
        boundaries.setSecond(Bytes.toBytes(k2));
      } else {
        boundaries.setFirst(Bytes.toBytes(k2));
        boundaries.setSecond(Bytes.toBytes(k1));
      }
      return boundaries;
    }

  }

}
