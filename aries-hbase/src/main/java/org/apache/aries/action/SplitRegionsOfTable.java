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

public class SplitRegionsOfTable extends SplitTable {

  public final static String SPLIT_RATIO = "split_table_regions.ratio";

  private float ratio;
  private List<HRegionInfo> regions;

  public SplitRegionsOfTable() {}

  @Override
  public void init(Configuration configuration, Connection connection) throws IOException {
    super.init(configuration, connection);
    ratio = configuration.getFloat("cr." + SPLIT_RATIO, 0.2f);
  }

  @Override
  protected void perform(TableName table) throws Exception {
    int number_regions = (int) (regions.size() * ratio);
    for (int i = 0; i < number_regions; i++) {
      HRegionInfo picked = regions.get(RANDOM.nextInt(regions.size()));
      LOG.info("Splitting region " + picked.getRegionNameAsString() + " of " + table);
      admin.splitRegion(picked.getRegionName());
    }
  }

  @Override
  protected void prePerform(TableName table) throws Exception {
    regions = admin.getTableRegions(table);
  }

  @Override
  protected void postPerform(TableName table) throws Exception {
    LOG.info("Performed split (it is an async call, don't know when will finish)");
  }

}
