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

import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.ToyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class UnbalanceRegions extends Action {

  public final static String      UNBALANCE_RATIO = "unbalance_region.ratio";

  private final Random RANDOM = new Random();

  private Admin admin;
  private float ratio;
  private int sleep_secs;

  public UnbalanceRegions() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    admin = connection.getAdmin();
    ratio = configuration.getFloat("cr." + UNBALANCE_RATIO, 0.2f);
  }

  @Override
  public Integer call() throws Exception {
    try {
      ClusterStatus status = admin.getClusterStatus();
      int total_region_num = status.getRegionsCount();
      int to_be_moved = (int) (total_region_num * ratio);

      List<ServerName> servers = new ArrayList<>(status.getServers());
      List<byte[]> regions = new ArrayList<>(total_region_num);

      for (ServerName server : servers) {
        ServerLoad load = status.getLoad(server);
        regions.addAll(load.getRegionsLoad()
                           .keySet()
                           .stream()
                           .map(r -> Bytes.toBytes(HRegionInfo.encodeRegionName(r)))
                           .collect(Collectors.toList()));
      }

      while (to_be_moved-- > 0) {
        ServerName picked_server = servers.get(RANDOM.nextInt(servers.size()));
        byte[] picked_region = regions.get(RANDOM.nextInt(regions.size()));
        LOG.info("Moving " + Bytes.toStringBinary(picked_region) + " to " + picked_server.getHostAndPort());
      }
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return RETURN_CODE.FAILURE.code();
    }
    return RETURN_CODE.SUCCESS.code();
  }

}
