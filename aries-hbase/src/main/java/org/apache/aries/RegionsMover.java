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

import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RegionsMover extends AbstractHBaseToy {

  private final Parameter<String[]> source_servers =
      StringArrayParameter.newBuilder("rm.source_servers").setRequired()
          .setDescription("Regions on these server will be unloaded, then reloaded. Please use server:port format, delimited by ','").opt();
  private final Parameter<String[]> target_servers =
      StringArrayParameter.newBuilder("rm.target_servers").setRequired()
          .setDescription("These servers will be used for store the regions from target servers temporarily or permanently.").opt();
  private final Parameter<Integer> thread_pool_size =
      IntParameter.newBuilder("rm.threads_for_move_regions").setDefaultValue(8).setDescription("Number of threads for moving regions.").opt();
  private final Parameter<Enum> move_or_reload =
      EnumParameter.newBuilder("rm.move_or_reload", MODE.RELOAD, MODE.class)
          .setDescription("MOVE: move regions from A to B. RELOAD: move regions from A to B, then from B to A.").opt();
  private final Parameter<Boolean> batch_move =
      BoolParameter.newBuilder("rm.batch_move", false).setDescription("By default move is one pair by one pair, set true to run in batch. This only applies to RELOAD").opt();

  enum MODE {
    MOVE, RELOAD
  }

  @Override protected String getParameterPrefix() {
    return "rm";
  }

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(source_servers);
    requisites.add(target_servers);
    requisites.add(thread_pool_size);
    requisites.add(move_or_reload);
    requisites.add(batch_move);
  }

  @Override protected void exampleConfiguration() {
    example(source_servers.key(), "source_server_1.com:5678,source_server_2.com:5678");
    example(target_servers.key(), "target_server_1.com:5678,target_server_2.com:5678");
    example(thread_pool_size.key(), "8");
    example(move_or_reload.key(), "RELOAD");
    example(batch_move.key(), "false");
  }

  Admin admin;
  ExecutorService pool;
  MODE mode;

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
    pool = Executors.newFixedThreadPool(thread_pool_size.value());
    mode = (MODE) move_or_reload.value();
    LOG.info("Using mode: " + mode.name());
  }

  @Override
  protected void midCheck() {
    if (target_servers.value().length != source_servers.value().length) {
      throw new IllegalArgumentException("Target servers size should be equal to temp servers size");
    }
  }

  @Override protected int haveFun() throws Exception {
    ServerName source;
    ServerName target;
    List<HRegionInfo> regions;

    int size = source_servers.value().length;
    switch (mode) {
      case MOVE: {
        for (int i = 0; i < size; i++) {
          source = findServer(source_servers.value()[i]);
          target = findServer(target_servers.value()[i]);
          regions = ProtobufUtil.getOnlineRegions(HConnectionManager.getConnection(connection.getConfiguration()).getAdmin(source));
          LOG.info("There are " + regions.size() + " regions on " + source);
          unloadRegionsTo(regions, target);
        }
        break;
      }
      case RELOAD: {
        if (batch_move.value()) {
          for (int i = 0; i < size; i++) {
            source = findServer(source_servers.value()[i]);
            target = findServer(target_servers.value()[i]);
            regions = ProtobufUtil.getOnlineRegions(HConnectionManager.getConnection(connection.getConfiguration()).getAdmin(source));
            LOG.info("There are " + regions.size() + " regions on " + source);
            unloadRegionsTo(regions, target);
          }
          promptForConfirm();
          for (int i = 0; i < size; i++) {
            source = findServer(source_servers.value()[i]);
            target = findServer(target_servers.value()[i]);
            regions = ProtobufUtil.getOnlineRegions(HConnectionManager.getConnection(connection.getConfiguration()).getAdmin(target));
            reloadRegionsTo(regions, source);
          }
        } else {
          for (int i = 0; i < size; i++) {
            source = findServer(source_servers.value()[i]);
            target = findServer(target_servers.value()[i]);
            regions = ProtobufUtil.getOnlineRegions(HConnectionManager.getConnection(connection.getConfiguration()).getAdmin(source));
            LOG.info("There are " + regions.size() + " regions on " + source);
            unloadRegionsTo(regions, target);
            promptForConfirm();
            // in case source is restarted
            source = findServer(source_servers.value()[i]);
            reloadRegionsTo(regions, source);
          }
        }
        break;
      }
    }
    return RETURN_CODE.SUCCESS.code();
  }

  private void promptForConfirm() {
    Scanner scanner = new Scanner(System.in);
    while (true) {
      LOG.info("After making sure target server is up, please enter y/Y to proceed regions reload: ");
      String y = scanner.nextLine();
      if (y.equalsIgnoreCase("y")) {
        return;
      }
    }
  }

  private ServerName findServer(String server) throws IOException {
    for (ServerName sn : admin.getClusterStatus().getServers()) {
      if (sn.getHostAndPort().equals(server)) {
        return sn;
      }
    }
    return null;
  }

  private void unloadRegionsTo(List<HRegionInfo> regions, ServerName server) throws Exception {
    move(regions, server);
    LOG.info("Unload regions finished!");
  }

  private void reloadRegionsTo(List<HRegionInfo> regions,ServerName server) throws Exception {
    // Like it is restarted, startcode will get updated.
    move(regions, server);
    LOG.info("Reload regions finished!");
  }

  private void move(List<HRegionInfo> regions, ServerName target) {
    AtomicInteger moved = new AtomicInteger(regions.size());
    for (HRegionInfo region : regions) {
      pool.submit(() -> {
        try {
          LOG.info("Moving " + region.getEncodedName() + " to " + target.getServerName());
          admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(target.getServerName()));
          LOG.info("Moved " + region.getEncodedName() + " to " + target.getServerName());
          moved.decrementAndGet();
        } catch (IOException e) {
          LOG.info("Error in moving " + region.getEncodedName());
        }
      });
    }
    while (moved.get() != 0);
  }

  @Override protected void destroyToy() throws Exception {
    admin.close();
    super.destroyToy();
  }

}
