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

package org.apache.aries.chaos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class MoveRegionsOfTable extends TableBase {

  public final static String MOVE_RATIO = "move_table_regions.ratio";

  private float ratio;
  private int number_regions;
  private List<HRegionInfo> regions;
  private ServerName[] servers;

  public MoveRegionsOfTable() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    ratio = configuration.getFloat("cr." + MOVE_RATIO, 0.2f);
  }

  @Override
  protected void perform(TableName table) throws Exception {
    for (int i = 0; i < number_regions; i++) {
      HRegionInfo picked = regions.get(RANDOM.nextInt(regions.size()));
      ServerName server = servers[RANDOM.nextInt(servers.length)];

      LOG.info("Moving region " + picked.getRegionNameAsString() + " of " + table + " to " + server);
      admin.move(picked.getRegionName(), Bytes.toBytes(server.getServerName()));
    }
  }

  @Override
  protected void prePerform(TableName table) throws Exception {
    super.prePerform(table);
    regions = admin.getTableRegions(table);
    servers = admin.getClusterStatus().getServers().toArray(new ServerName[0]);
    number_regions = (int) (regions.size() * ratio);
  }

  @Override
  protected void postPerform(TableName table) throws Exception {
    super.postPerform(table);
    LOG.info("Finish moving " + number_regions + " of " + table + " in " + getDuration() + " seconds");
  }

}
