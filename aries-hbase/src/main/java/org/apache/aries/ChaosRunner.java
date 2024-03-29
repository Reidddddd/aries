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

import org.apache.aries.chaos.Action;
import org.apache.aries.chaos.AlterBase;
import org.apache.aries.chaos.BatchRestartRegionServer;
import org.apache.aries.chaos.CompactRegionsOfTable;
import org.apache.aries.chaos.FlushRegionsOfTable;
import org.apache.aries.chaos.MergeRegionsOfTable;
import org.apache.aries.chaos.MoveRegionsOfTable;
import org.apache.aries.chaos.RestartBase;
import org.apache.aries.chaos.RestartBase.Signal;
import org.apache.aries.chaos.RestartDataNode;
import org.apache.aries.chaos.RestartMaster;
import org.apache.aries.chaos.RestartRegionServer;
import org.apache.aries.chaos.RestartZookeeper;
import org.apache.aries.chaos.RollingRestartRegionServer;
import org.apache.aries.chaos.SplitRegionsOfTable;
import org.apache.aries.chaos.TableBase;
import org.apache.aries.chaos.UnbalanceRegions;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.FloatParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
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
      IntParameter.newBuilder("cr.concurrency").setDefaultValue(1).setRequired()
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
  private final Parameter<String> policy =
      StringParameter.newBuilder("cr.action_policy").setDefaultValue("RANDOM").setRequired()
                     .addConstraint(p -> p.equals("RANDOM") || p.equals("SEQUENCE"))
                     .setDescription("Mode for action execution: RANDOM or SEQUENCE. RANDOM will execute the listed actions randomly," +
                         " while SEQUENCE will execute the actions as the order written. RANDOM by default").opt();

  private final Parameter<Boolean> ignore_failure =
      BoolParameter.newBuilder("cr.ignore.failure.if_action_faied", false)
          .setDescription("If set true, when a chaos action fails with some reasons, it will continue next chaos action," +
              "            by default false which means aborting the whole run")
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
  // RestartRegionServer & RollingRestartRegionServer & BatchRestartRegionServer
  public final Parameter<String> rs_start_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_START)
                     .setDescription("Command to start regionserver").opt();
  public final Parameter<String> rs_check_stopped_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_CHECK_STOPPED_COMMAND)
                     .setDescription("Command to check whether regionserver is dead").opt();
  private final Parameter<Integer> rs_status_timeout =
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartRegionServer.RS_STATUS_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for regionserver to dead or alive, in seconds").opt();
  private final Parameter<Integer> rolling_max_dead=
      IntParameter.newBuilder(getParameterPrefix() + "." + RollingRestartRegionServer.ROLLING_RS_MAX_DEAD)
                  .setDefaultValue(2)
                  .setDescription("Max number of dead regionserver while doing rolling restart").opt();
  private final Parameter<Integer> rolling_sleep_time=
      IntParameter.newBuilder(getParameterPrefix() + "." + RollingRestartRegionServer.ROLLING_RS_SLEEP_BETWEEN)
                  .setDefaultValue(2)
                  .setDescription("Sleep time between batch operations, in seconds").opt();
  private final Parameter<Boolean> rolling_meta_exclude =
      BoolParameter.newBuilder(getParameterPrefix() + "." + RollingRestartRegionServer.ROLLING_RS_EXCLUDE_META, true)
                   .setDescription("Whether meta regionserver should be excluded, during rolling restart, true by default.")
                   .opt();
  private final Parameter<Boolean> rolling_async_mode =
      BoolParameter.newBuilder(getParameterPrefix() + "." + RollingRestartRegionServer.ROLLING_RS_ASYNC, true)
                   .setDescription("Async mode will not wait for the stop action finished, true by default.")
                   .opt();
  private final Parameter<Integer> batch_restart_num =
      IntParameter.newBuilder(getParameterPrefix() + "." + BatchRestartRegionServer.BATCH_RS_RESTART_NUM)
                  .setDefaultValue(3)
                  .setDescription("Number of regionservers to be batch restarted").opt();
  private final Parameter<Integer> batch_sleep_time=
      IntParameter.newBuilder(getParameterPrefix() + "." + BatchRestartRegionServer.BATCH_RS_SLEEP_TIME)
                  .setDefaultValue(5)
                  .setDescription("Sleep time before start regionservers just killed, in seconds").opt();
  private final Parameter<Boolean> batch_meta_exclude =
      BoolParameter.newBuilder(getParameterPrefix() + "." + BatchRestartRegionServer.BATCH_RS_EXCLUDE_META, true)
                   .setDescription("Whether meta regionserver should be excluded, during batch restart, true by default.")
                   .opt();
  // RestartMaster
  public final Parameter<String> mst_start_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_START)
                     .setDescription("Command to start master").opt();
  public final Parameter<String> mst_check_stopped_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_CHECK_STOPPED_COMMAND)
                     .setDescription("Command to check whether master is dead").opt();
  private final Parameter<Integer> mst_status_timeout =
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartMaster.MS_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for master to dead or alive, in seconds").opt();
  // RestartDataNode
  public final Parameter<String> dn_start_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartDataNode.DN_START)
                     .setDescription("Command to start datanode").opt();
  private final Parameter<Integer> dn_status_timeout =
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartDataNode.DN_STATUS_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for datanode to dead or alive, in seconds").opt();
  // RestartZookeeper
  public final Parameter<String> zk_start_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_START)
                     .setDescription("Command to start zookeeper").opt();
  public final Parameter<String> zk_check_status_cmd =
      StringParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_CHECK_STATUS_COMMAND)
                     .setDescription("Command to check whether zookeeper is alive or dead").opt();
  private final Parameter<Integer> zk_status_timeout =
      IntParameter.newBuilder(getParameterPrefix() + "." +  RestartZookeeper.ZK_TIMEOUT).setDefaultValue(0)
                  .setDescription("Timeout waiting for zookeeper to dead or alive, in seconds").opt();

  // TableBase
  public final Parameter<String> table_name =
      StringParameter.newBuilder(getParameterPrefix() + "." +  TableBase.TABLE_NAME)
                     .setDescription("Table to perform chaos action, if unspecified, random pick one existed table").opt();
  private final Parameter<Integer> act_rounds =
      IntParameter.newBuilder(getParameterPrefix() + "." +  TableBase.X_ROUNDS).setDefaultValue(2)
                  .setDescription("Rounds to perform actions, 2 by default").opt();
  private final Parameter<Integer> sleep_between_table_action =
      IntParameter.newBuilder(getParameterPrefix() + "." +  TableBase.SLEEP_SECS).setDefaultValue(1)
                  .setDescription("Sleep time between each chaos action round, in seconds, 1 secs by default").opt();
  // CompactRegionsOfTable
  private final Parameter<Float> compact_table_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + CompactRegionsOfTable.COMPACT_RATIO)
                    .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
                    .setDescription("A ratio of regions to be compacted").opt();
  // FlushRegionsOfTable
  private final Parameter<Float> flush_table_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + FlushRegionsOfTable.FLUSH_RATIO)
                    .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
                    .setDescription("A ratio of regions to be flushed").opt();
  // SplitRegionsOfTable
  private final Parameter<Float> split_table_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + SplitRegionsOfTable.SPLIT_RATIO)
                    .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
                    .setDescription("A ratio of regions to be splitted").opt();
  // MoveRegionsOfTable
  private final Parameter<Float> move_table_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + MoveRegionsOfTable.MOVE_RATIO)
                    .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
                    .setDescription("A ratio of regions to be moved").opt();
  // MergeRegionsOfTable
  private final Parameter<Float> merge_table_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + MergeRegionsOfTable.MERGE_RATIO)
                    .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
                    .setDescription("A ratio of regions to be merged").opt();

  // AlterBase
  public final Parameter<String> alter_table_name =
      StringParameter.newBuilder(getParameterPrefix() + "." +  AlterBase.TABLE_NAME)
          .setDescription("Table to perform alter action, if unspecified, random pick one existed table").opt();
  public final Parameter<String> alter_family_name =
      StringParameter.newBuilder(getParameterPrefix() + "." +  AlterBase.FAMILY_NAME)
          .setDescription("Family of the specfied table to perform alter action, if unspecified, pick the first one (in alphabet order)").opt();

  // Others
  private final Parameter<Float> unbalance_region_ratio =
      FloatParameter.newBuilder(getParameterPrefix() + "." + UnbalanceRegions.UNBALANCE_RATIO)
          .setDefaultValue(0.2f).addConstraint(r -> r > 0).addConstraint(r -> r < 1.0)
          .setDescription("A ratio of regions to be unbalanced").opt();


  private final Random random = new Random();
  private final ExecutorService exe = Executors.newCachedThreadPool();

  private Action[] chaos_actions;
  private ActionPolicy action_policy;
  private Semaphore semaphore;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    chaos_actions = new Action[chaos.value().length];
    semaphore = new Semaphore(concurrency.value());
    int i = 0;
    for (String chao : chaos.value()) {
      Action action = (Action) Class.forName("org.apache.aries.chaos." + chao).newInstance();
      chaos_actions[i++] = action;
      action.init(hbase_conf, connection);
    }
    action_policy = policy.value().equals("RANDOM") ? new RandomPick() : new SequencePick();
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
    requisites.add(policy);
    requisites.add(ignore_failure);

    // Restart base
    requisites.add(local_exe_path);
    requisites.add(kill_signal);
    requisites.add(sleep_a_while);

    requisites.add(rs_start_cmd);
    requisites.add(rs_status_timeout);
    requisites.add(rs_check_stopped_cmd);
    requisites.add(rolling_max_dead);
    requisites.add(rolling_sleep_time);
    requisites.add(rolling_meta_exclude);
    requisites.add(rolling_async_mode);
    requisites.add(batch_restart_num);
    requisites.add(batch_sleep_time);
    requisites.add(batch_meta_exclude);

    requisites.add(mst_start_cmd);
    requisites.add(mst_check_stopped_cmd);
    requisites.add(mst_status_timeout);

    requisites.add(dn_start_cmd);
    requisites.add(dn_status_timeout);

    requisites.add(zk_start_cmd);
    requisites.add(zk_status_timeout);
    requisites.add(zk_check_status_cmd);


    // Table base
    requisites.add(table_name);
    requisites.add(act_rounds);
    requisites.add(sleep_between_table_action);
    requisites.add(compact_table_region_ratio);
    requisites.add(flush_table_region_ratio);
    requisites.add(split_table_region_ratio);
    requisites.add(move_table_region_ratio);
    requisites.add(merge_table_region_ratio);


    // Alter base
    requisites.add(alter_table_name);
    requisites.add(alter_family_name);


    // Others
    requisites.add(unbalance_region_ratio);
  }

  @Override
  protected void exampleConfiguration() {
    example(concurrency.key(), "1");
    example(chaos.key(), "1");
    example(running_time.key(), "6000");
    example(policy.key(), "RANDOM");
    example(ignore_failure.key(), "false");

    // Restart base
    example(local_exe_path.key(), "/home/util/remote-ssh");
    example(kill_signal.key(), "SIGKILL");
    example(sleep_a_while.key(), "10");

    example(rs_start_cmd.key(), "sudo systemctl start regionserver");
    example(rs_status_timeout.key(), "10");
    example(rs_check_stopped_cmd.key(), "sudo systemctl is-active regionserver -q");
    example(rolling_max_dead.key(), "2");
    example(rolling_sleep_time.key(), "2");
    example(rolling_meta_exclude.key(), "true");
    example(rolling_async_mode.key(), "true");
    example(batch_restart_num.key(), "3");
    example(batch_sleep_time.key(), "5");
    example(batch_meta_exclude.key(), "true");

    example(mst_start_cmd.key(), "sudo systemctl start master");
    example(mst_status_timeout.key(), "120");
    example(mst_check_stopped_cmd.key(), "sudo systemctl is-active master -q");

    example(dn_start_cmd.key(), "sudo systemctl start datanode");
    example(dn_status_timeout.key(), "120");

    example(zk_start_cmd.key(), "sudo systemctl start master");
    example(zk_status_timeout.key(), "120");
    example(zk_check_status_cmd.key(), "sudo systemctl is-active master -q");


    // Table base
    example(table_name.key(), "NAMESPACE:table");
    example(act_rounds.key(), "2");
    example(sleep_between_table_action.key(), "1");
    example(compact_table_region_ratio.key(), "0.2");
    example(flush_table_region_ratio.key(), "0.2");
    example(split_table_region_ratio.key(), "0.2");
    example(move_table_region_ratio.key(), "0.2");
    example(merge_table_region_ratio.key(), "0.2");


    // Alter base
    example(alter_table_name.key(), "NAMESPACE:table");
    example(alter_family_name.key(), "f");


    // Others
    example(unbalance_region_ratio.key(), "0.2");
  }

  interface ActionPolicy {
    Action pickOneAction(Action[] actions);
  }

  class RandomPick implements ActionPolicy {
    @Override
    public Action pickOneAction(Action[] actions) {
      return actions[random.nextInt(actions.length)];
    }
  }

  class SequencePick implements ActionPolicy {
    private int last_pick = 0;

    @Override
    public Action pickOneAction(Action[] actions) {
      if (last_pick == actions.length) return null;
      return actions[last_pick++];
    }
  }

  @Override
  protected int haveFun() throws Exception {
    boolean timer = running_time.value() != -1;
    long now = System.currentTimeMillis();
    long future = now + TimeUnit.MILLISECONDS.convert(running_time.value(), TimeUnit.SECONDS);

    List<Future<Integer>> futures = new LinkedList<>();


    main:
    while (!timer || System.currentTimeMillis() < future) {
      while (semaphore.availablePermits() > 0) {
        semaphore.acquire(1);
        Action action = action_policy.pickOneAction(chaos_actions);
        if (action == null) break main;
        Future<Integer> res = exe.submit(action);
        futures.add(res);
      }

      Iterator<Future<Integer>> it = futures.iterator();
      boolean noRelease = true;
      while (it.hasNext()) {
        Future<Integer> f = it.next();
        if (f.isDone()) {
          semaphore.release(1);
          it.remove();
          if (f.get() == RETURN_CODE.FAILURE.code()) {
            if (ignore_failure.value()) {
              LOG.info("Ignore failure and run next chaos action");
              continue;
            }
            LOG.warning("Exiting...");
            return RETURN_CODE.FAILURE.code();
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
      if (remaining.get() == RETURN_CODE.FAILURE.code()) {
        LOG.warning("Exiting...");
        return RETURN_CODE.FAILURE.code();
      }
    }

    return RETURN_CODE.SUCCESS.code();
  }

}
