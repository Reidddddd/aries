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
import org.apache.aries.action.RestartBase.Signal;
import org.apache.aries.action.RestartDataNode;
import org.apache.aries.action.RestartMaster;
import org.apache.aries.action.RestartRegionServer;
import org.apache.aries.action.RestartZookeeper;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
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
  private final Parameter<Enum> kill_signal =
      EnumParameter.newBuilder(getParameterPrefix() + "." + RestartBase.KILL_SIGNAL, Signal.SIGKILL, Signal.class)
                   .setDescription("Kill signal for stopping processes, supports SIGKILL or SIGTERM only, SIGKILL by default")
                   .opt();
  private final Parameter<Integer> sleep_a_while =
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.SLEEP_A_WHILE).setDefaultValue(0)
                  .setDescription("Sleep for seconds before executing start command").opt();
  // RestartRegionServer
  public final Parameter<String> start_rs_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_START)
                     .setDescription("Command to start regionserver").opt();
  public final Parameter<String> check_rs_stopped_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_CHECK_STOPPED_COMMAND)
                     .setDescription("Command to check whether regionserver is dead").opt();
  private final Parameter<Integer> chao_rs_timeout=
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for regionserver to dead or alive, in seconds").opt();
  // RestartMaster
  public final Parameter<String> start_mst_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_START)
                     .setDescription("Command to start master").opt();
  public final Parameter<String> check_mst_stopped_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_CHECK_STOPPED_COMMAND)
                     .setDescription("Command to check whether master is dead").opt();
  private final Parameter<Integer> chao_mst_timeout=
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for master to dead or alive, in seconds").opt();
  // RestartDataNode
  public final Parameter<String> start_dn_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartDataNode.DN_START)
                     .setDescription("Command to start datanode").opt();
  private final Parameter<Integer> chao_dn_timeout=
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartDataNode.DN_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for datanode to dead or alive, in seconds").opt();
  // RestartZookeeper
  public final Parameter<String> start_zk_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_START)
                     .setDescription("Command to zookeeper master").opt();
  public final Parameter<String> check_zk_status_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_CHECK_STATUS_COMMAND)
                     .setDescription("Command to check whether zookeeper is alive or dead").opt();
  private final Parameter<Integer> chao_zk_timeout=
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for zookeeper to dead or alive, in seconds").opt();

  private final Random random = new Random();
  private final int ERROR = 1;

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
    requisites.add(kill_signal);
    requisites.add(sleep_a_while);

    requisites.add(start_rs_cmd);
    requisites.add(chao_rs_timeout);
    requisites.add(check_rs_stopped_cmd);

    requisites.add(start_mst_cmd);
    requisites.add(check_mst_stopped_cmd);
    requisites.add(chao_mst_timeout);

    requisites.add(start_dn_cmd);
    requisites.add(chao_dn_timeout);

    requisites.add(start_zk_cmd);
    requisites.add(chao_zk_timeout);
    requisites.add(check_zk_status_cmd);
  }

  @Override
  protected void exampleConfiguration() {
    example(concurrency.key(), "1");
    example(chaos.key(), "1");
    example(running_time.key(), "6000");

    example(local_exe_path.key(), "/home/util/remote-ssh");
    example(kill_signal.key(), "SIGKILL");
    example(sleep_a_while.key(), "10");

    example(start_rs_cmd.key(), "sudo systemctl start regionserver");
    example(chao_rs_timeout.key(), "10");
    example(check_rs_stopped_cmd.key(), "sudo systemctl is-active regionserver -q");

    example(start_mst_cmd.key(), "sudo systemctl start master");
    example(chao_mst_timeout.key(), "120");
    example(check_mst_stopped_cmd.key(), "sudo systemctl is-active master -q");

    example(start_dn_cmd.key(), "sudo systemctl start datanode");
    example(chao_dn_timeout.key(), "120");

    example(start_zk_cmd.key(), "sudo systemctl start master");
    example(chao_zk_timeout.key(), "120");
    example(check_zk_status_cmd.key(), "sudo systemctl is-active master -q");
  }

  @Override
  protected int haveFun() throws Exception {
    boolean timer = running_time.value() != -1;
    long now = System.currentTimeMillis();
    long future = now + TimeUnit.MILLISECONDS.convert(running_time.value(), TimeUnit.SECONDS);

    List<Future<Integer>> futures = new LinkedList<>();


    while (!timer || System.currentTimeMillis() < future) {
      while (semaphore.availablePermits() > 0) {
        int i = random.nextInt(chaos_actions.length);
        semaphore.acquire(1);
        Future<Integer> res = exe.submit(chaos_actions[i]);
        futures.add(res);
      }

      Iterator<Future<Integer>> it = futures.iterator();
      boolean noRelease = true;
      while (it.hasNext()) {
        Future<Integer> f = it.next();
        if (f.isDone()) {
          semaphore.release(1);
          it.remove();
          if (f.get() == ERROR) {
            LOG.warning("Exiting...");
            System.exit(1);
          }
          noRelease = false;
          break;
        }
      }

      if (noRelease) Thread.sleep(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));
      else {
        LOG.info("--------------- Sleep(2s) before next chaos action ---------------");
        Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS));
      }
    }

    for (Future<Integer> remaining : futures) {
      if (remaining.get() == ERROR) {
        LOG.warning("Exiting...");
        System.exit(1);
      }
    }

    return 0;
  }

}
