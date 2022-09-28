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

import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class RemoteSSH {

  protected static final Logger LOG = Logger.getLogger(RemoteSSH.class.getName());

  private ShellCommandExecutor exe;

  private RemoteSSH(String exePath, String host, String command) {
    String[] exes = { exePath, host, command };
    exe = new ShellCommandExecutor(exes);
  }

  public void run() throws IOException {
    LOG.info("Executing " + Arrays.toString(exe.getExecString()));
    exe.execute();
  }

  public String getOutput() {
    return exe.getOutput();
  }

  public static class RemoteSSHBuilder {

    private String exePath;
    private String remoteHost;
    private String command;

    private RemoteSSHBuilder() {}

    public RemoteSSHBuilder setCommand(String command) {
      this.command = command;
      return this;
    }

    public RemoteSSHBuilder setExePath(String exePath) {
      this.exePath = exePath;
      return this;
    }

    public RemoteSSHBuilder setRemoteHost(String remoteHost) {
      this.remoteHost = remoteHost;
      return this;
    }

    public static RemoteSSHBuilder newBuilder() {
      return new RemoteSSHBuilder();
    }

    public RemoteSSH build() {
      if (   exePath == null) throw new IllegalArgumentException("exePath must be set");
      if (remoteHost == null) throw new IllegalArgumentException("remoteHost must be set");
      if (   command == null) throw new IllegalArgumentException("command must be set");
      return new RemoteSSH(exePath, remoteHost, command);
    }

  }

}
