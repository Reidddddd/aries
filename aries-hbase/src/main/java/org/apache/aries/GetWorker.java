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

import org.apache.aries.common.Constants;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GetWorker extends AbstractHBaseToy {

  private final Parameter<Integer> num_connections =
      IntParameter.newBuilder("gw.num_connections").setRequired()
                  .setDescription("Number of connections used for get")
                  .addConstraint(v -> v > 0).opt();
  private final Parameter<String> table_name =
      StringParameter.newBuilder("gw.target_table").setRequired()
                     .setDescription("A table that data will be read").opt();
  private final Parameter<String> family =
      StringParameter.newBuilder("gw.target_family")
                     .setDescription("A family that belongs to the target_table, and wanted to be read")
                     .setRequired().opt();
  private final Parameter<Integer> running_time =
      IntParameter.newBuilder("gw.running_time").setDescription("How long this application run (in seconds").opt();
  private final Parameter<Enum> value_kind =
      EnumParameter.newBuilder("gw.value_kind", VALUE_KIND.FIXED, VALUE_KIND.class)
                   .setDescription("After the value read, it will be used to verify the result").opt();
  private final Parameter<Integer> key_length =
      IntParameter.newBuilder("gw.key_length").setDefaultValue(Constants.DEFAULT_KEY_LENGTH_PW)
                  .setDescription("The length of the generated key in bytes.").opt();
  private final Parameter<Enum> key_kind =
      EnumParameter.newBuilder("gw.key_kind", KEY_PREFIX.NONE, KEY_PREFIX.class)
                   .setDescription("Key prefix type: NONE, HEX, DEC.").opt();

  enum VALUE_KIND {
    RANDOM, FIXED
  }

  enum KEY_PREFIX {
    NONE, DEC, HEX
  }

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
  private MessageDigest digest;

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(num_connections);
    requisites.add(table_name);
    requisites.add(family);
    requisites.add(running_time);
    requisites.add(value_kind);
    requisites.add(key_kind);
  }

  @Override
  protected void exampleConfiguration() {
    example(num_connections.key(), "3");
    example(table_name.key(), "table:for_put");
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
    Worker[] workers = new Worker[num_connections.value()];
    for (int i = 0; i < num_connections.value(); i++) {
      workers[i] = new Worker(configuration);
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

    digest = MessageDigest.getInstance("MD5");
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

  @Override protected String getParameterPrefix() {
    return "pw";
  }

  class Worker implements Runnable {

    Connection connection;
    long empty_get;
    long effective_get;
    long correct_get;
    long wrong_get;

    Worker(ToyConfiguration conf) throws IOException {
      connection = createConnection(conf);
      LOG.info("Connection created " + connection);
    }

    Connection createConnection(ToyConfiguration conf) throws IOException {
      Configuration hbase_conf = ConfigurationFactory.createHBaseConfiguration(conf);
      return ConnectionFactory.createConnection(hbase_conf);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(table);
        while (running) {
          String key = getKey();
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
            verifiedResult(kind, key, value);
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

    private String getKey() {
      String key = ToyUtils.generateRandomString(key_length.value());
      switch (key_prefix) {
        case HEX: {
          byte[] digested = digest.digest(Bytes.toBytes(key));
          String md5 = new BigInteger(1, digested).toString(16).toLowerCase();
          return md5.substring(0, 2) + ":" + key;
        }
        case DEC: {
          byte[] digested = digest.digest(Bytes.toBytes(key));
          String md5 = new BigInteger(1, digested).toString(10).toLowerCase();
          return md5.substring(md5.length() - 2) + ":" + key;
        }
        case NONE: {
          return key;
        }
        default:
          throw new IllegalStateException("Unexpected value: " + key_prefix);
      }
    }

    private void verifiedResult(VALUE_KIND kind, String key, byte[] value) {
      if (kind == VALUE_KIND.RANDOM) {
        return;
      }

      int res = Bytes.compareTo(value, ToyUtils.generateBase64Value(key));
      if (res == 0) {
        correct_get++;
      } else {
        wrong_get++;
      }
    }

  }

}
