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

package org.apache.aries.common;

import org.apache.aries.AbstractHBaseToy;
import org.apache.aries.ToyConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class BaseWorker extends AbstractHBaseToy {

  private final Parameter<Integer> num_connections =
      IntParameter.newBuilder(getParameterPrefix() + ".num_connections").setRequired()
                  .setDescription("Number of connections used for worker")
                  .addConstraint(v -> v > 0).opt();
  private final Parameter<String> table_name =
      StringParameter.newBuilder(getParameterPrefix() + ".target_table").setRequired()
                     .setDescription("A table to be processed").opt();
  protected final Parameter<String> family =
      StringParameter.newBuilder(getParameterPrefix() + ".target_family")
                     .setDescription("A family that belongs to the target_table, and wanted to be processed")
                     .setRequired().opt();
  protected final Parameter<Integer> running_time =
      IntParameter.newBuilder(getParameterPrefix() + ".running_time").setDescription("How long this application run (in seconds").opt();
  private final Parameter<Enum> value_kind =
      EnumParameter.newBuilder(getParameterPrefix() + ".value_kind", VALUE_KIND.FIXED, VALUE_KIND.class)
                   .setDescription("After the value read, it will be used to verify the result").opt();
  protected final Parameter<Integer> key_length =
      IntParameter.newBuilder(getParameterPrefix() + ".key_length").setDefaultValue(Constants.DEFAULT_KEY_LENGTH_PW)
                  .setDescription("The length of the generated key in bytes.").opt();
  private final Parameter<Enum> key_kind =
      EnumParameter.newBuilder(getParameterPrefix() + ".key_kind", KEY_PREFIX.NONE, KEY_PREFIX.class)
                   .setDescription("Key prefix type: NONE, HEX, DEC.").opt();

  private ExecutorService service;
  private final Object mutex = new Object();

  protected TableName table;
  protected Admin admin;
  protected VALUE_KIND kind;
  protected KEY_PREFIX key_prefix;
  protected volatile boolean running = true;

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(num_connections);
    requisites.add(table_name);
    requisites.add(family);
    requisites.add(running_time);
    requisites.add(value_kind);
    requisites.add(key_kind);
    requisites.add(key_length);
  }

  @Override
  protected void exampleConfiguration() {
    example(num_connections.key(), "3");
    example(table_name.key(), "table:for_process");
    example(family.key(), "f");
    example(running_time.key(), "300");
    example(value_kind.key(), "FIXED");
    example(key_length.key(), "10");
    example(key_kind.key(), "NONE");
  }

  protected abstract BaseHandler createHandler(ToyConfiguration configuration) throws IOException;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    table = TableName.valueOf(table_name.value());
    admin = connection.getAdmin();
    if (!admin.tableExists(table)) {
      throw new TableNotFoundException(table);
    }

    service = Executors.newFixedThreadPool(num_connections.value());
    BaseHandler[] workers = new BaseHandler[num_connections.value()];
    for (int i = 0; i < num_connections.value(); i++) {
      workers[i] = createHandler(configuration);
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
    return 0;
  }

  @Override
  protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

}
