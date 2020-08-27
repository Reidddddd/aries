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
import org.apache.aries.common.StringParameter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("rawtypes")
public class MergeTable extends AbstractHBaseToy {

  private final Parameter<String> merge_table_url =
      StringParameter.newBuilder("mt.url").setRequired().setDescription("The pages of the table. Please copy from HBase Web UI").opt();
  private final Parameter<Integer> merge_size_threshold =
      IntParameter.newBuilder("mt.merge_threshold_megabytes").setDefaultValue(100)
                  .setDescription("Regions under this threshold will be merged, unit in MB").opt();
  private final Parameter<Integer> runs_interval_sec =
      IntParameter.newBuilder("mt.run_interval_sec").setDefaultValue(600).setDescription("Interval between merge run, in seconds").opt();
  private final Parameter<String> merge_condition =
      StringParameter.newBuilder("mt.merge_condition").setDefaultValue("all").setRequired()
                     .setDescription("Merge condition, there're 3 options: all, size, rreq, "
                         + "all contains size and rreq. size condition needs to have mt.merge_threshold_megabytes set, 100 by default."
                         + " rreq is short for read request which needs mt.merge_threshold_readrequest set, 0 by default.").opt();
  private final Parameter<Integer> merge_rreq_threshold =
      IntParameter.newBuilder("mt.merge_threshold_readrequest").setDefaultValue(0)
                  .setDescription("Regions read request under this threshold will be merged.").opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(merge_table_url);
    requisites.add(merge_condition);
    requisites.add(merge_size_threshold);
    requisites.add(merge_rreq_threshold);
    requisites.add(runs_interval_sec);
  }

  @Override
  protected void exampleConfiguration() {
    example(merge_table_url.key(), "http://host:port/table.jsp?name=namespace:table");
    example(merge_condition.key(), "all");
    example(merge_size_threshold.key(), "100");
    example(merge_rreq_threshold.key(), "0");
    example(runs_interval_sec.key(), "500");
  }

  Admin admin;
  TableName table;
  long threshold_bytes;
  int read_requests;
  int round = Constants.UNSET_INT;

  final Conditions conditions = new Conditions();
  final MergeCondition size_condition = region -> region.getSizeInBytes() < threshold_bytes;
  final MergeCondition rreq_condition = region -> region.readRequests() <= read_requests;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
    int start = merge_table_url.value().indexOf("=") + 1;
    table = TableName.valueOf(merge_table_url.value().substring(start));

    String type = merge_condition.value();
    if (type.equalsIgnoreCase("size")) {
      threshold_bytes = merge_size_threshold.value() * Constants.ONE_MB;
      conditions.addCondition(size_condition);
    } else if (type.equalsIgnoreCase("rreq")) {
      read_requests = merge_rreq_threshold.value();
      conditions.addCondition(rreq_condition);
    } else if (type.equalsIgnoreCase("all")) {
      threshold_bytes = merge_size_threshold.value() * Constants.ONE_MB;
      conditions.addCondition(size_condition);
      read_requests = merge_rreq_threshold.value();
      conditions.addCondition(rreq_condition);
    }
  }

  @Override
  protected int haveFun() throws Exception {
    do {
      List<HRegionInfo> regions = admin.getTableRegions(table);
      Document doc = Jsoup.connect(merge_table_url.value()).get();
      Element element = doc.getElementById("regionServerDetailsTable");
      TableInfo table_info = new TableInfo(element);
      if (round == Constants.UNSET_INT) {
        // It is determined by first run.
        round = calculateHowManyRuns(table_info);
      }
      for (int i = 0, index_a, index_b; i < table_info.regionNum(); ) {
        index_a = i++;
        index_b = i++;
        if (index_b >= table_info.regionNum()) {
          break;
        }
        RegionInfo region_A = table_info.getRegionAtIndex(index_a);
        RegionInfo region_B = table_info.getRegionAtIndex(index_b);
        if (conditions.shouldMerge(region_A, region_B)) {
          HRegionInfo A_region = regions.get(index_a);
          HRegionInfo B_region = regions.get(index_b);
          LOG.info("Merging region " + A_region.getRegionId() + " and " + B_region.getRegionId());
          admin.mergeRegions(
              A_region.getEncodedNameAsBytes(),
              B_region.getEncodedNameAsBytes(),
              false);
        }
      }
      LOG.info("Sleeping for " + runs_interval_sec.value() + " seconds to wait for CatalogJanitor cleaning merged regions.");
      TimeUnit.SECONDS.sleep(runs_interval_sec.value());
    } while (--round != 0);
    return 0;
  }

  private int calculateHowManyRuns(TableInfo table) {
    int qualified_for_merge = 0;
    for (RegionInfo region : table.getRegions()) {
      qualified_for_merge += conditions.shouldMerge(region) ? 1 : 0;
    }
    int result = (int) (Math.log(qualified_for_merge) / Math.log(2));
    LOG.info("There will be " + result + " runs");
    return result;
  }

  @Override
  protected void destroyToy() throws Exception {
    admin.close();
    super.destroyToy();
  }

  @Override protected String getParameterPrefix() {
    return "mt";
  }

  interface MergeCondition {
    boolean shouldMerge(RegionInfo region);
  }

  class Conditions {
    List<MergeCondition> conditions = new ArrayList<>();

    void addCondition(MergeCondition condition) {
      conditions.add(condition);
    }

    public boolean shouldMerge(RegionInfo region_A, RegionInfo region_B) {
      for (MergeCondition condition : conditions) {
        if (condition.shouldMerge(region_A) || condition.shouldMerge(region_B)) {
          return true;
        }
      }
      return false;
    }

    public boolean shouldMerge(RegionInfo region) {
      for (MergeCondition condition : conditions) {
        if (condition.shouldMerge(region)) {
          return true;
        }
      }
      return false;
    }
  }

  class RegionInfo {

    String name;
    String server;
    String read_requests;
    String write_requests;
    String file_size;
    String file_num;
    String mem_size;
    String locality;
    String start_key;
    String end_key;

    RegionInfo(Element region) {
      Elements column = region.select("td");
               int i = 0;
      name           = column.get(i++).text();
      server         = column.get(i++).text();
      read_requests  = column.get(i++).text();
      write_requests = column.get(i++).text();
      file_size      = column.get(i++).text();
      file_num       = column.get(i++).text();
      mem_size       = column.get(i++).text();
      locality       = column.get(i++).text();
      start_key      = column.get(i++).text();
      end_key        = column.get(i++).text();
    }

    public long getSizeInBytes() {
      long size;
      String num = file_size.split(" ")[0];
      if (file_size.contains("GB")) {
        float gb = Float.parseFloat(num); // get the number
        size = (long) (gb * Constants.ONE_GB);
      } else if (file_size.contains("MB")) {
        float mb = Float.parseFloat(num);
        size = (long) (mb * Constants.ONE_MB);
      } else if (file_size.contains("KB")) {
        float kb = Float.parseFloat(num);
        size = (long) (kb * Constants.ONE_KB);
      } else {
        size = Long.parseLong(num);
      }
      return size;
    }

    public int readRequests() {
      return Integer.parseInt(read_requests.replaceAll(",", ""));
    }

    @Override
    public String toString() {
      return "RegionInfo{" +
          "name='" + name + '\'' +
          ", server='" + server + '\'' +
          ", read_requests='" + read_requests + '\'' +
          ", write_requests='" + write_requests + '\'' +
          ", file_size='" + file_size + '\'' +
          ", file_num='" + file_num + '\'' +
          ", mem_size='" + mem_size + '\'' +
          ", locality='" + locality + '\'' +
          ", start_key='" + start_key + '\'' +
          ", end_key='" + end_key + '\'' +
          '}';
    }

  }

  class TableInfo {

    final List<RegionInfo> regions;

    TableInfo(Element table) {
      Elements rows = table.select("tr");
      regions = new ArrayList<>(rows.size() - 1); // Skip first title row
      for (int i = 1; i < rows.size(); i++) {
        regions.add(new RegionInfo(rows.get(i)));
      }
    }

    public List<RegionInfo> getRegions() {
      return regions;
    }

    public int regionNum() {
      return regions.size();
    }

    RegionInfo getRegionAtIndex(int i) {
      return regions.get(i);
    }

  }

}
