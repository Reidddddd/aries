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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;

public class FlushRegionsOfTable extends FlushTable {

  public final static String FLUSH_RATIO = "flush_table_regions.ratios";

  private float ratio;
  private List<HRegionInfo> regions;
  private int number_regions;

  public FlushRegionsOfTable() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    ratio = configuration.getFloat("cr." + FLUSH_RATIO, 0.2f);
  }

  @Override
  protected void perform(TableName table) throws Exception {
    for (int i = 0; i < number_regions; i++) {
      HRegionInfo picked = regions.get(RANDOM.nextInt(regions.size()));
      LOG.info("Start flushing region " + picked.getRegionNameAsString() + " of " + table);
      admin.flushRegion(picked.getRegionName());
    }
  }

  @Override
  protected void prePerform(TableName table) throws Exception {
    super.prePerform(table);
    regions = admin.getTableRegions(table);
    number_regions = (int) (regions.size() * ratio);
  }

  @Override
  protected void postPerform(TableName table) throws Exception {
    super.postPerform(table);
    LOG.info("Finish flushing " + number_regions + " of " + table + " in " + getDuration() + " seconds");
  }

}
