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

import org.apache.aries.common.Constants;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.RegionInfo;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.StringParameter;
import org.apache.aries.common.TableInfo;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SplitTable extends AbstractHBaseToy {

  private final Parameter<String> split_table_url =
      StringParameter.newBuilder("st.url").setDescription("The pages of the table. Please copy from HBase Web UI").opt();
  private final Parameter<Integer> split_size_threshold =
      IntParameter.newBuilder("st.merge_threshold_megabytes").setDefaultValue(10240)
                  .setDescription("Regions above this threshold will be splited, unit in MB").opt();
  private final Parameter<String[]> tables =
      StringArrayParameter.newBuilder("st.table_name").setRequired()
          .setDescription("Tables's names to be dropped, delimited by ','. Pattern is supported, by prefixing '#'")
          .addConstraint(v -> v.length > 0).opt();
  private final Parameter<String[]> regions =
      StringArrayParameter.newBuilder("st.regions_of_table")
          .setDescription("Regions's encoded names of a table, delimited by ','. If specified, st.table_name size must be 1.")
          .opt();

  @Override protected String getParameterPrefix() {
    return "st";
  }

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(split_table_url);
    requisites.add(split_size_threshold);
    requisites.add(tables);
    requisites.add(regions);
  }

  @Override protected void exampleConfiguration() {
    example(split_table_url.key(), "http://host:port/table.jsp?name=namespace:table");
    example(split_size_threshold.key(), "10240");
    example(tables.key(), "ns1:t1");
    example(regions.key(), "24221b13bcd6c3f86c75c64ebdf688f2,28e8df1df7540441e125bd8d748252b0");
  }

  Admin admin;
  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();

    if (!split_size_threshold.empty()) {
      threshold_bytes = split_size_threshold.value() * Constants.ONE_MB;
    }
  }

  @Override protected void midCheck() {
    if (!regions.empty() && tables.value().length > 1) {
      throw new IllegalArgumentException("When " + regions.key() + " is set, we can split only one table at one time only, for safety concerns");
    }
  }

  long threshold_bytes;

  @Override protected int haveFun() throws Exception {
    if (!regions.empty()) {
      String table = tables.value()[0];
      for (String region : regions.value()) {
        LOG.info("Splitting table " + table + "'s region " + region);
        admin.splitRegion(Bytes.toBytes(region));
      }
      return RETURN_CODE.SUCCESS.code();
    }

    if (!split_table_url.empty()) {
      int start = split_table_url.value().indexOf("=") + 1;
      TableName table = TableName.valueOf(split_table_url.value().substring(start));
      List<HRegionInfo> regions = admin.getTableRegions(table);
      Document doc = Jsoup.connect(split_table_url.value()).get();
      Element element = doc.getElementById("regionServerDetailsTable");
      TableInfo table_info = new TableInfo(element);
      for (int i = 0; i < table_info.regionNum(); i++) {
        RegionInfo region = table_info.getRegionAtIndex(i);
        if (region.getSizeInBytes() > threshold_bytes) {
          HRegionInfo target_region = regions.get(i);
          LOG.info("Splitting region " + target_region.getEncodedName());
          admin.splitRegion(target_region.getEncodedNameAsBytes());
        }
      }
      return RETURN_CODE.SUCCESS.code();
    }

    List<TableName> pending = new ArrayList<>();
    for (String table_or_pattern : tables.value()) {
      if (table_or_pattern.startsWith("#")) {
        pending.addAll(Arrays.asList(admin.listTableNames(Pattern.compile(table_or_pattern.substring(1)))));
      } else {
        pending.add(TableName.valueOf(table_or_pattern));
      }
    }

    for (TableName name : pending) {
      for (HRegionInfo region : admin.getTableRegions(name)) {
        LOG.info("Splitting table " + name + "'s region " + region.getEncodedName());
        admin.splitRegion(region.getEncodedNameAsBytes());
      }
    }
    return RETURN_CODE.SUCCESS.code();
  }

  @Override protected void destroyToy() throws Exception {
    admin.close();
    super.destroyToy();
  }

}
