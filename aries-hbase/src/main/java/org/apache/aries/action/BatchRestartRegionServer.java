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

import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.ArrayList;

public class BatchRestartRegionServer extends RestartRegionServer {

  public static final String  BATCH_RS_RESTART_NUM = "batch.restart_regionserver.number";
  public static final String   BATCH_RS_SLEEP_TIME = "batch.restart_regionserver.sleep_in_seconds.before.restart";
  public static final String BATCH_RS_EXCLUDE_META = "batch.restart_regionserver.exclude.meta.regionserver";

  private        int num_restart;
  private        int sleep_seconds;
  private    boolean exclude_meta;
  private ServerName meta_regionserver;

  public BatchRestartRegionServer() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
             service_type = ServiceType.REGIONSERVER;
          num_restart = configuration.getInt("cr." + BATCH_RS_RESTART_NUM, 3);
        sleep_seconds = configuration.getInt("cr." + BATCH_RS_SLEEP_TIME, 0);
         exclude_meta = configuration.getBoolean("cr." + BATCH_RS_EXCLUDE_META, true);
    meta_regionserver = connection.getRegionLocator(TableName.META_TABLE_NAME)
                                  .getRegionLocation(HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
                                  .getServerName();
  }

  @Override
  protected void chaos() throws Exception {
    ArrayList<ServerName> to_be_killed = new ArrayList<>(num_restart);
    for (int i = 0; i < num_restart;) {
      ServerName server = super.pickTargetServer();
      if (exclude_meta) {
        if (server.getHostname().equals(meta_regionserver.getHostname()) &&
            server.getPort() == meta_regionserver.getPort()) {
          continue;
        }
      }
      if (to_be_killed.contains(server)) continue;
      to_be_killed.add(i++, server);
    }

    for (ServerName server : to_be_killed) stopProcess(server);
    for (ServerName server : to_be_killed) waitingStopped(server);

    Thread.sleep(ToyUtils.getTimeoutInMilliSeconds(sleep_seconds));

    for (ServerName server : to_be_killed) startProcess(server);
    for (ServerName server : to_be_killed) waitingStarted(server);
  }

}
