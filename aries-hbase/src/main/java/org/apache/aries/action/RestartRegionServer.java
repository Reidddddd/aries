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

import org.apache.aries.RemoteSSH;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;

public class RestartRegionServer extends RestartBase {

  public static final String                 RS_START = "restart_regionserver.start_command";
  public static final String               RS_TIMEOUT = "restart_regionserver.status.check.timeout_in_seconds";
  public static final String RS_CHECK_STOPPED_COMMAND = "restart_regionserver.check.stopped_command";

  private String start_cmd;
  private String check_stopped_command;
  private int timeout;

  public RestartRegionServer() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
             service_type = ServiceType.REGIONSERVER;
                start_cmd = configuration.get("cr." + RS_START);
    check_stopped_command = configuration.get("cr." + RS_CHECK_STOPPED_COMMAND);
                  timeout = configuration.getInt("cr." + RS_TIMEOUT, 0);
  }

  @Override
  public String startCommand() {
    return start_cmd;
  }

  @Override
  public long getTimeout() {
    return getTimeoutInMilliSeconds(timeout);
  }

  @Override
  protected ServerName pickTargetServer() throws Exception {
    ClusterStatus status = admin.getClusterStatus();
    ServerName[] servers = status.getServers().toArray(new ServerName[0]);

    return servers[random.nextInt(servers.length)];
  }

  @Override
  protected void waitingStarted(ServerName target_server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to start on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      for (ServerName server : admin.getClusterStatus().getServers()) {
        if (server.getHostname().equals(target_server.getHostname()) &&
            server.getPort() == target_server.getPort()) {
          LOG.info(service_type.service() + " on " + server.getHostname() + " is started");
          return;
        }
      }
      Threads.sleep(500);
    }
    String err = "Timeout waiting for " + service_type.service() + " to start on " + target_server.getHostname();
    LOG.warning(err);
    throw new IOException(err);
  }

  protected void waitingStopped(ServerName target_server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to stop on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(check_stopped_command)
                                  .setRemoteHost(target_server.getHostname())
                                  .build();
    while (System.currentTimeMillis() < future) {
      try {
        remote_ssh.run();
      } catch (Exception ece) {
        if (remote_ssh.exitCode() == 0) continue;
        LOG.info(service_type.service() + " on " + target_server.getHostname() + " is stopped");
        return;
      }
      Threads.sleep(getTimeout() / 5);
    }
    String err = "Timeout waiting for " + service_type.service() + " to stop on " + target_server.getHostname();
    LOG.warning(err);
    throw new IOException(err);
  }

}
