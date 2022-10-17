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

package org.apache.aries.action;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.LinkedList;

public class RollingRestartRegionServer extends RestartRegionServer {

  public static final String ROLLING_RS_SLEEP_BETWEEN = "rolling.restart_regionserver.sleep.between.action.time_in_seconds";
  public static final String  ROLLING_RS_EXCLUDE_META = "rolling.restart_regionserver.exclude.meta.regionserver";
  public static final String      ROLLING_RS_MAX_DEAD = "rolling.restart_regionserver.max.dead.servers";
  public static final String         ROLLING_RS_ASYNC = "rolling.restart_regionserver.async.mode";

  private final LinkedList<ServerName> dead_servers = new LinkedList<>();

  private        int sleep_between;
  private        int max_dead;
  private    boolean exclude_meta;
  private    boolean async;
  private ServerName meta_regionserver;

  enum Action {
    STOP, START, DEFAULT
  }

  public RollingRestartRegionServer() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
        sleep_between = configuration.getInt("cr." + ROLLING_RS_SLEEP_BETWEEN, 0);
             max_dead = configuration.getInt("cr." + ROLLING_RS_MAX_DEAD, 3);
         exclude_meta = configuration.getBoolean("cr." + ROLLING_RS_EXCLUDE_META, true);
                async = configuration.getBoolean("cr." + ROLLING_RS_ASYNC, true);
    meta_regionserver = connection.getRegionLocator(TableName.META_TABLE_NAME)
                                  .getRegionLocation(HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
                                  .getServerName();
  }

  @Override
  protected ServerName pickTargetServer() throws Exception {
    ServerName target_server;
    while (true) {
      target_server = super.pickTargetServer();
      if (exclude_meta) {
        if (!target_server.getHostname().equals(meta_regionserver.getHostname())) break;
        if (target_server.getPort() != meta_regionserver.getPort())               break;
      }                                                                      else break;
    }
    return target_server;
  }

  @Override
  protected void chaos() throws Exception {
    boolean just_start = true;

    while (dead_servers.size() < max_dead) {
      if (just_start) just_start = false;
      else Thread.sleep(getTimeoutInMilliSeconds(sleep_between));

      ServerName target_server = pickTargetServer();
      if (dead_servers.isEmpty()) {
        stopProcess(target_server);
        if (!async) waitingStopped(target_server);
        dead_servers.add(target_server);
        continue;
      }

      if (async && dead_servers.contains(target_server)) continue;

      int act_code = random.nextInt(3);
      Action action = act_code == 0 ? Action.START :
                      act_code == 1 ? Action.STOP  : Action.DEFAULT;
      switch (action) {
        case START: {
          ServerName start_server = dead_servers.removeFirst();
          if (async) waitingStopped(start_server);
          startProcess(start_server);
          waitingStarted(start_server);
          break;
        }
        case    STOP:
        case DEFAULT: {
          stopProcess(target_server);
          if (!async) waitingStopped(target_server);
          dead_servers.add(target_server);
          break;
        }
      }
    }

    for (ServerName dead_server : dead_servers) {
      waitingStopped(dead_server);
      startProcess(dead_server);
    }
    for (ServerName dead_server : dead_servers) {
      waitingStarted(dead_server);
    }
    dead_servers.clear();
  }

}
