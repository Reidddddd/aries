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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;

public class RestartZookeeper extends RestartBase {

  public static final String                 ZK_START = "restart_zookeeper.start_command";
  public static final String               ZK_TIMEOUT = "restart_zookeeper.status.check.timeout_in_seconds";
  public static final String  ZK_CHECK_STATUS_COMMAND = "restart_zookeeper.check.status_command";

  private String start_cmd;
  private String check_status_command;
  private int timeout;

  public RestartZookeeper() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
             service_type = ServiceType.QuorumPeerMain;
                start_cmd = configuration.get("cr." + ZK_START);
     check_status_command = configuration.get("cr." + ZK_CHECK_STATUS_COMMAND);
                  timeout = configuration.getInt("cr." + ZK_TIMEOUT, 0);
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
  protected String findPidCommand(ServiceType service) {
    return String.format("ps ux | grep %s | grep -v grep | tr -s ' ' | cut -d ' ' -f2", service.name());
  }

  @Override
  protected ServerName pickTargetServer() throws Exception {
    String[] zks = connection.getConfiguration().getStrings(HConstants.ZOOKEEPER_QUORUM);
    String zookeeper = zks[random.nextInt(zks.length)];
    return ServerName.valueOf(zookeeper, -1, -1);
  }

  @Override
  protected void waitingStarted(ServerName target_server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to start on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(check_status_command)
                                  .setRemoteHost(target_server.getHostname())
                                  .build();
    while (System.currentTimeMillis() < future) {
      try {
        remote_ssh.run();
        if (remote_ssh.exitCode() == 0) {
          // should be, just double check here
          LOG.info(service_type.service() + " on " + target_server.getHostname() + " is started");
        }
        return;
      } catch (Exception ece) {
        // ignore, keep waiting
      }
      Threads.sleep(sleepInterval());
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
                                  .setCommand(check_status_command)
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
      Threads.sleep(sleepInterval());
    }
    String err = "Timeout waiting for " + service_type.service() + " to stop on " + target_server.getHostname();
    LOG.warning(err);
    throw new IOException(err);
  }

}
