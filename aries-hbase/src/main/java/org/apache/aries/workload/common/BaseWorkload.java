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

package org.apache.aries.workload.common;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.aries.AbstractHBaseToy;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Constants;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.MetricRegistryInstance;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringParameter;
import org.apache.aries.common.ToyUtils;
import org.apache.aries.workload.PutWorkload;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.workload.factory.HandlerFactory;
import org.apache.aries.workload.factory.HandlerFactory.BaseHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class BaseWorkload extends AbstractHBaseToy {

  protected final Parameter<Integer> num_connections =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + "num_connections")
                  .setRequired()
                  .setDescription("Number of connections used for worker")
                  .addConstraint(v -> v > 0)
                  .opt();
  private final Parameter<String> table_name =
      StringParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + "target_table")
                     .setRequired()
                     .setDescription("A table to be processed")
                     .opt();
  protected final Parameter<Integer> report_interval =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + "report_interval")
                  .setDefaultValue(1)
                  .setDescription("The interval for metrics output to console, in seconds.")
                  .opt();
  protected final Parameter<Integer> running_time =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + "running_time")
                  .setDescription("How long this application run (in seconds")
                  .opt();
  protected final Parameter<String> family =
      StringParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.FAMILY)
                     .setDescription("A family that belongs to the target_table, and wanted to be processed")
                     .setRequired()
                     .opt();
  private final Parameter<Enum> value_kind =
      EnumParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.VALUE_KINE, VALUE_KIND.FIXED, VALUE_KIND.class)
                   .setDescription("After the value read, it will be used to verify the result")
                   .opt();
  protected final Parameter<Integer> key_length =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.KEY_LENGTH)
                  .setDefaultValue(Constants.DEFAULT_KEY_LENGTH_PW)
                  .setDescription("The length of the generated key in bytes.")
                  .opt();
  protected final Parameter<Enum> key_kind =
      EnumParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.KEY_KIND, KEY_PREFIX.NONE, KEY_PREFIX.class)
                   .setDescription("Key prefix type: NONE, HEX, DEC, SEQ.")
                   .opt();
  protected final Parameter<Integer> records_num =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.RECORDS_NUM)
                  .setDescription("How many records will be put or read under SEQ key kind.")
                  .opt();
  private final Parameter<Boolean> shared_connection =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + BaseHandler.SHARED_CONNECTION, false)
                   .setDescription("If set true, all connections in fact will share one connection underlying")
                   .opt();

  private final Object mutex = new Object();
  private final MetricRegistry registry = MetricRegistryInstance.getMetricRegistry();
  private final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                                          .convertRatesTo(TimeUnit.SECONDS)
                                                          .convertDurationsTo(TimeUnit.NANOSECONDS)
                                                          .build();

  private ExecutorService service;
  private BaseHandler[] handlers;
  private CountDownLatch latch;

  protected TableName table;
  protected Admin admin;
  protected VALUE_KIND kind;
  protected KEY_PREFIX key_prefix;

  @Override
  public void requisite(List<Parameter> requisites) {
    requisites.add(num_connections);
    requisites.add(table_name);
    requisites.add(family);
    requisites.add(running_time);
    requisites.add(value_kind);
    requisites.add(key_kind);
    requisites.add(key_length);
    requisites.add(records_num);
    requisites.add(report_interval);
    requisites.add(shared_connection);
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
    example(records_num.key(), "10000");
    example(report_interval.key(), "10");
    example(shared_connection.key(), "true");
  }

  @Override
  protected void midCheck() {
    KEY_PREFIX prefix = (KEY_PREFIX) key_kind.value();
    if (prefix == KEY_PREFIX.SEQ) {
      if (records_num.empty()) {
        throw new IllegalArgumentException("If use SEQ key kind, " + getParameterPrefix() + ".records_num must be set.");
      }
    }
  }

  protected abstract HandlerFactory initHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters);

  public interface Callback {

    void onFinished();

  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    table = TableName.valueOf(table_name.value());
    admin = connection.getAdmin();
    if (!admin.tableExists(table)) {
      throw new TableNotFoundException(table);
    }

    HandlerFactory factory = initHandlerFactory(configuration, getParameters());
    kind = (VALUE_KIND) value_kind.value();
    key_prefix = (KEY_PREFIX) key_kind.value();

    service = Executors.newFixedThreadPool(num_connections.value(), new ThreadFactory() {
      int i = 0;
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Handler-" + (i++));
      }
    });

    reporter.start(report_interval.value(), TimeUnit.SECONDS);

    // 1. Create Handler firsts
    handlers = new BaseHandler[num_connections.value()];
    for (int i = 0; i < handlers.length; i++) {
      handlers[i] = factory.createHandler(table);
    }
    // 2. Create end callback if this is a PutWorkload
    if (this.getClass().equals(PutWorkload.class)) {
      latch = new CountDownLatch(num_connections.value());
      for (int i = 0; i < handlers.length; i++) {
        handlers[i].setCallback(() -> {
          if (latch != null) {
            latch.countDown();
          }
        });
      }
    }
    // 3. Submit handlers
    for (int i = 0; i < handlers.length; i++) {
      service.submit(handlers[i]);
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

    if (latch != null) {
      service.submit(() -> {
        try {
          latch.await();
        } catch (InterruptedException e) {
          // igonre and just return
          return;
        }

        synchronized (mutex) {
          mutex.notify();
        }
      });
    }

  }

  @Override
  protected int haveFun() throws Exception {
    synchronized (mutex) {
      mutex.wait();
      for (BaseHandler handler : handlers) {
        handler.interrupt();
      }
    }
    service.awaitTermination(30, TimeUnit.SECONDS);
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
    reporter.report();
    reporter.close();
  }

}
