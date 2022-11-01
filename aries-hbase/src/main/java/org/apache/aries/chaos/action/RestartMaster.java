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

package org.apache.aries.chaos.action;

import org.apache.aries.RemoteSSH;
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;

public class RestartMaster extends RestartBase {

  public static final String                 MS_START = "restart_master.start_command";
  public static final String               MS_TIMEOUT = "restart_master.status.check.timeout_in_seconds";
  public static final String MS_CHECK_STOPPED_COMMAND = "restart_master.check.stopped_command";

  private String start_cmd;
  private String check_stopped_command;
  private int timeout;

  public RestartMaster() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
             service_type = ServiceType.MASTER;
                start_cmd = configuration.get("cr." + MS_START);
    check_stopped_command = configuration.get("cr." + MS_CHECK_STOPPED_COMMAND);
                  timeout = configuration.getInt("cr." + MS_TIMEOUT, 0);
  }

  @Override
  public String startCommand() {
    return start_cmd;
  }

  @Override
  public long getTimeout() {
    return ToyUtils.getTimeoutInMilliSeconds(timeout);
  }

  @Override
  protected ServerName pickTargetServer() throws Exception {
    return admin.getClusterStatus().getMaster();
  }

  @Override
  protected void waitingStarted(ServerName target_server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to start on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      for (ServerName backup_master : admin.getClusterStatus().getBackupMasters()) {
        if (backup_master.getHostname().equals(target_server.getHostname()) &&
            backup_master.getPort() == target_server.getPort()) {
          LOG.info(service_type.service() + " on " + target_server.getHostname() + " is started");
          return;
        }
      }
      Threads.sleep(sleepInterval());
    }
    String err = "Timeout waiting for " + service_type.service() + " to start on " + target_server.getHostname();
    LOG.warning(err);
    throw new IOException(err);
  }

  protected void waitingStopped(ServerName target_server) throws IOException {
    long future = System.currentTimeMillis() + getTimeout();

    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(check_stopped_command)
                                  .setRemoteHost(target_server.getHostname())
                                  .build();

    while (System.currentTimeMillis() < future) {
      try {
        ((ClusterConnection)this.connection).getMaster();
        try {
          if (admin.getClusterStatus().getMaster().equals(target_server)) {
            // double check
            throw new IOException("Master hasn't switched yet");
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
        LOG.info("Master switched successfully");

        // master switch, check process killed clean
        LOG.info("Waiting for " + service_type.service() + " to stop on " + target_server.getHostname());
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

      } catch (IOException m) {
        LOG.warning(m.getMessage());
      }
      Threads.sleep(sleepInterval());
    }
    String err = "Timeout waiting for active master switched";
    LOG.warning(err);
    throw new IOException(err);
  }

}
