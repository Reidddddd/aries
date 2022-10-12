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
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class RestartBase extends Action {

  public static String REMOTE_SSH_EXE_PATH = "restart_base_action.remote_ssh.exe.path";
  public static String         KILL_SIGNAL = "restart_base_action.kill.signal";
  public static String       SLEEP_A_WHILE = "restart_base_action.sleep.seconds.before_start";

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

  protected final Random random = new Random();

  protected ServiceType service_type;
  protected Signal signal;
  protected int sleep_a_while;
  protected Admin admin;
  protected String remote_ssh_exe_path;

  private String error = "";

  public RestartBase() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
                  admin = connection.getAdmin();
    remote_ssh_exe_path = configuration.get("cr." + REMOTE_SSH_EXE_PATH);
                 signal = configuration.getEnum("cr." + KILL_SIGNAL, Signal.SIGKILL);
          sleep_a_while = configuration.getInt("cr." + SLEEP_A_WHILE, 1);
  }

  protected abstract ServerName pickTargetServer() throws Exception;

  @Override
  public Integer call() throws Exception {
    ServerName target_server = pickTargetServer();

    try {
      stopProcess(target_server);
      waitingStopped(target_server);
      Thread.sleep(getTimeoutInMilliSeconds(sleep_a_while));
      startProcess(target_server);
      waitingStarted(target_server);
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return 1;
    }

    return 0;
  }

  private void startProcess(ServerName target_server) throws IOException {
    LOG.info("Starting " + service_type.service() + " on " + target_server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(startCommand())
                                  .setRemoteHost(target_server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  private void stopProcess(ServerName target_server) throws IOException {
    LOG.info("Stopping " + service_type.service() + " on " + target_server.getHostname());
    RemoteSSH.RemoteSSHBuilder builder = RemoteSSH.RemoteSSHBuilder.newBuilder();
    RemoteSSH remote_ssh = builder.setExePath(remote_ssh_exe_path)
                                  .setCommand(signalCommand(service_type, signal))
                                  .setRemoteHost(target_server.getHostname())
                                  .build();
    remote_ssh.run();
  }

  protected String findPidCommand(ServiceType service) {
    return String.format("ps ux | grep proc_%s | grep -v grep | tr -s ' ' | cut -d ' ' -f2", service.procName());
  }

  protected String signalCommand(ServiceType service, Signal signal) {
    return String.format("%s | xargs kill -s %s", findPidCommand(service), signal);
  }

  protected long getTimeoutInMilliSeconds(int timeout_in_seconds) {
    return TimeUnit.MILLISECONDS.convert(timeout_in_seconds, TimeUnit.SECONDS);
  }

  public abstract String startCommand();

  public abstract long getTimeout();

  protected abstract void waitingStarted(ServerName target_server) throws IOException;

  protected abstract void waitingStopped(ServerName target_server) throws IOException;

}
