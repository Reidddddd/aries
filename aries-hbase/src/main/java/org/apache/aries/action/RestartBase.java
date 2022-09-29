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
import java.util.Locale;
import java.util.Random;

public abstract class RestartBase extends Action {

  public static String REMOTE_SSH_EXE_PATH = "restart_base_action.remote_ssh.exe.path";
  public static String KILL_SIGNAL = "restart_base_action.kill.signal";

  enum ServiceType {
    MASTER("Master"),
    REGIONSERVER("RegionServer");

    private String name;

    ServiceType(String name) {
      this.name = name;
    }

    public String procName() {
      return name.toLowerCase(Locale.ROOT);
    }

    public String service() {
      return name;
    }
  }

  public enum Signal {
    SIGKILL, SIGTERM
  }

  private final Random random = new Random();

  protected ServiceType service_type;
  protected Signal signal;

  protected Admin admin;

  private String remote_ssh_exe_path;

  public RestartBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    admin = connection.getAdmin();
    remote_ssh_exe_path = configuration.get("cr." + REMOTE_SSH_EXE_PATH);
    signal = configuration.getEnum("cr." + KILL_SIGNAL, Signal.SIGKILL);
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
    waitingStarted(target_server);

    return 0;
  }

  private void waitingStopped(ServerName server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to stop on " + server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      if (!checkAlive(server.getHostname())) {
        LOG.info(service_type.service() + " on " + server.getHostname() + " is stopped");
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
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(isRunningCommand(service_type))
                                  .setRemoteHost(hostname)
                                  .build();
    remote_ssh.run();
    return remote_ssh.getOutput().trim().length() > 0;
  }

  private void startProcess(ServerName server) throws IOException {
    LOG.info("Starting " + service_type.service() + " on " + server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(startCommand())
                                  .setRemoteHost(server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  private void stopProcess(ServerName server) throws IOException {
    LOG.info("Stopping " + service_type.service() + " on " + server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(signalCommand(service_type, signal))
                                  .setRemoteHost(server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  protected String findPidCommand(ServiceType service) {
    return String.format("ps ux | grep proc_%s | grep -v grep | tr -s ' ' | cut -d ' ' -f2", service.procName());
  }

  public String isRunningCommand(ServiceType service) {
    return findPidCommand(service);
  }

  protected String signalCommand(ServiceType service, Signal signal) {
    return String.format("%s | xargs kill -s %s", findPidCommand(service), signal);
  }

  public abstract String startCommand();

  public abstract long getTimeout();

  protected abstract void waitingStarted(ServerName target_server) throws IOException;

}
