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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class RestartBase extends Action {

  public static String REMOTE_SSH_EXE_PATH = "remote_ssh.exe.path";

  enum ServiceType {
    HBASE_MASTER("Master"),
    HBASE_REGIONSERVER("RegionServer");

    private String name;

    ServiceType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  private final Random random = new Random();

  protected ServiceType service_type;

  protected Admin admin;

  private String remoteSSHExePath;

  public RestartBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    admin = connection.getAdmin();
    remoteSSHExePath = configuration.get("cr." + REMOTE_SSH_EXE_PATH);
  }

  @Override
  public Integer call() throws Exception {
    ClusterStatus status = admin.getClusterStatus();
    ServerName[] servers = status.getServers().toArray(new ServerName[0]);

    int pick = random.nextInt(servers.length);
    ServerName target_server = servers[pick];

    stopProcess(target_server);
    waitingStopped(target_server);
    startProcess(target_server);
    checkStarted(target_server);

    return 0;
  }

  private void waitingStopped(ServerName server) throws IOException {
    LOG.info("Waiting for " + service_type.getName() + " to stop on " + server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      if (!checkAlive(server.getHostname())) {
        LOG.info(service_type.getName() + " on " + server.getHostname() + " is stopped");
        return;
      }
      Threads.sleep(100);
    }
    String err = "Timeout waiting for " + service_type.name + " to stop on " + server;
    LOG.warning(err);
    throw new IOException(err);
  }

  private boolean checkAlive(String hostname) throws IOException {
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remoteSSHExePath)
                                  .setCommand(getCheckAliveCommand())
                                  .setRemoteHost(hostname)
                                  .build();
    remote_ssh.run();
    return Integer.parseInt(remote_ssh.getOutput().trim()) > 0;
  }

  private void startProcess(ServerName server) throws IOException {
    LOG.info("Starting " + service_type.getName() + " on " + server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remoteSSHExePath)
                                  .setCommand(getStartCommand())
                                  .setRemoteHost(server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  private void stopProcess(ServerName server) throws IOException {
    LOG.info("Stopping " + service_type.getName() + " on " + server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remoteSSHExePath)
                                  .setCommand(getStopCommand())
                                  .setRemoteHost(server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  public abstract String getStartCommand();

  public abstract String getStopCommand();

  public abstract String getCheckAliveCommand();

  public abstract long getTimeout();

  protected abstract void checkStarted(ServerName target_server) throws IOException;

}
