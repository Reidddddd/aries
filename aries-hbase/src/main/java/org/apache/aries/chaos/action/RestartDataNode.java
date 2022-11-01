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

import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class RestartDataNode extends RestartBase {

  public static final String          DN_START = "restart_datanode.start_command";
  public static final String DN_STATUS_TIMEOUT = "restart_datanode.status.check.timeout_in_seconds";

  private String start_cmd;
  private int timeout;
  private DistributedFileSystem file_system = new DistributedFileSystem();

  public RestartDataNode() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
             service_type = ServiceType.DATANODE;
                start_cmd = configuration.get("cr." + DN_START);
                  timeout = configuration.getInt("cr." + DN_STATUS_TIMEOUT, 0);
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
    Configuration conf = connection.getConfiguration();
    file_system.initialize(FileSystem.getDefaultUri(conf), conf);
    DFSClient dfs_client = file_system.getClient();
    List<ServerName> servers = new LinkedList<>();
    for (DatanodeInfo dataNode: dfs_client.datanodeReport(HdfsConstants.DatanodeReportType.LIVE)) {
      servers.add(ServerName.valueOf(dataNode.getHostName(), dataNode.getIpcPort(), -1));
    }
    return servers.get(RANDOM.nextInt(servers.size()));
  }

  @Override
  protected void waitingStarted(ServerName target_server) throws IOException {
    LOG.info("Waiting for " + service_type.service() + " to start on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      DFSClient dfs_client = file_system.getClient();
      for (DatanodeInfo dataNode: dfs_client.datanodeReport(HdfsConstants.DatanodeReportType.LIVE)) {
        if (dataNode.getHostName().equals(target_server.getHostname()) &&
            dataNode.getIpcPort() == target_server.getPort()) {
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
    LOG.info("Waiting for " + service_type.service() + " to stop on " + target_server.getHostname());
    long future = System.currentTimeMillis() + getTimeout();

    while (System.currentTimeMillis() < future) {
      DFSClient dfs_client = file_system.getClient();
      for (DatanodeInfo dataNode: dfs_client.datanodeReport(HdfsConstants.DatanodeReportType.DEAD)) {
        if (dataNode.getHostName().equals(target_server.getHostname()) &&
            dataNode.getIpcPort() == target_server.getPort()) {
          LOG.info(service_type.service() + " on " + target_server.getHostname() + " is stopped");
          return;
        }
      }
      Threads.sleep(sleepInterval());
    }
    String err = "Timeout waiting for " + service_type.service() + " to stop on " + target_server.getHostname();
    LOG.warning(err);
    throw new IOException(err);
  }

}
