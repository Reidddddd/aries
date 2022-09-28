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

import org.apache.aries.action.Action;
import org.apache.aries.action.RestartBase;
import org.apache.aries.action.RestartRegionServer;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.StringParameter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ChaosRunner extends AbstractHBaseToy {

  private final Parameter<Integer> concurrency =
      IntParameter.newBuilder("cr.concurrency").setDefaultValue(1)
                  .setDescription("Number of chaos running simultaneously, 1 by default")
                  .opt();
  private final Parameter<String[]> chaos =
      StringArrayParameter.newBuilder("cr.chaos_to_run").setRequired()
                          .setDescription("Chao's names to be run, delimited by ','")
                          .addConstraint(v -> v.length > 0).opt();
  private final Parameter<Integer> running_time =
      IntParameter.newBuilder("cr.running_time")
                  .setDescription("How long this application run (in seconds), -1 means run forever unless user kills it")
                  .opt();

  // RestartBase
  public final Parameter<String> local_exe_path =
      StringParameter.newBuilder(getParameterPrefix() + "." + RestartBase.REMOTE_SSH_EXE_PATH)
                     .setDescription("Local path to execute remote SSH").opt();
  // RestartRegionServer
  public final Parameter<String> stop_rs_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." + RestartRegionServer.RS_STOP)
                     .setDescription("Command to stop regionserver").opt();
  public final Parameter<String> start_rs_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_START)
                     .setDescription("Command to start regionserver").opt();
  private final Parameter<String> check_rs_alive_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_ALIVE)
                     .setDescription("Command to check regionserver alive").opt();
  private final Parameter<Long> chao_rs_timeout=
      LongParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_TIMEOUT).setDefaultValue(0L)
                   .setDescription("Timeout waiting for regionserver to dead or alive, in seconds").opt();

  private final Random random = new Random();

  private ExecutorService exe = Executors.newCachedThreadPool();
  private Action[] chaos_actions;
  private Semaphore semaphore;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    chaos_actions = new Action[chaos.value().length];
    semaphore = new Semaphore(concurrency.value());
    int i = 0;
    for (String chao : chaos.value()) {
      Action action = (Action) Class.forName("org.apache.aries.action." + chao).newInstance();
      chaos_actions[i++] = action;
      action.init(hbase_conf, connection);
    }
  }

  @Override
  protected String getParameterPrefix() {
    return "cr";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(concurrency);
    requisites.add(chaos);
    requisites.add(running_time);

    requisites.add(local_exe_path);

    requisites.add(stop_rs_cmd);
    requisites.add(start_rs_cmd);
    requisites.add(check_rs_alive_cmd);
    requisites.add(chao_rs_timeout);
  }

  @Override
  protected void exampleConfiguration() {
    example(concurrency.key(), "1");
    example(chaos.key(), "1");
    example(running_time.key(), "6000");

    example(local_exe_path.key(), "/home/util/remote-ssh");

    example(stop_rs_cmd.key(), "sudo systemctl stop regionserver");
    example(start_rs_cmd.key(), "sudo systemctl start regionserver");
    example(check_rs_alive_cmd.key(), "jps | grep regionserver");
    example(chao_rs_timeout.key(), "10");
  }

  @Override
  protected int haveFun() throws Exception {
    boolean timer = running_time.value() != -1;
    long now = System.currentTimeMillis();
    long future = now + TimeUnit.NANOSECONDS.convert(running_time.value(), TimeUnit.SECONDS);

    List<Future> futures = new LinkedList<>();

    try {
      while (!timer || System.currentTimeMillis() < future) {
        while (semaphore.availablePermits() > 0) {
          int i = random.nextInt(chaos_actions.length);
          semaphore.acquire(1);
          Future<Integer> res = exe.submit(chaos_actions[i]);
          futures.add(res);
        }

        Iterator<Future> it = futures.iterator();
        boolean noRelease = true;
        while (it.hasNext()) {
          Future f = it.next();
          if (f.isDone()) {
            semaphore.release(1);
            it.remove();
            noRelease = false;
            break;
          }
        }

        if (noRelease) Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
      }
    } catch (Exception e) {
      LOG.info("Abort ChaosRunner due to " + e.getMessage());
    }

    return 0;
  }

}