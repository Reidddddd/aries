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
import org.apache.aries.common.Constants;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.RegionInfo;
import org.apache.aries.common.StringParameter;
import org.apache.aries.common.TableInfo;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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
  private final Parameter<Enum> condition_logic =
      EnumParameter.newBuilder("mt.condition_logic", LOGIC.OR, LOGIC.class)
                   .setDescription("Condition logic, either AND or OR. If AND, two regions should satisfy both, OR is either. OR by default").opt();
  private final Parameter<Boolean> hbase_version2 =
      BoolParameter.newBuilder("mt.hbase_version2", false)
                   .setDescription("Whether the table is in hbase version2 cluster, false by default.")
                   .opt();
  private final Parameter<Integer> thread_pool_size =
      IntParameter.newBuilder("mt.threads_for_merge_regions")
                  .setDefaultValue(4)
                  .setDescription("Number of threads for merging regions.")
                  .opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(merge_table_url);
    requisites.add(merge_condition);
    requisites.add(merge_size_threshold);
    requisites.add(merge_rreq_threshold);
    requisites.add(runs_interval_sec);
    requisites.add(condition_logic);
    requisites.add(hbase_version2);
    requisites.add(thread_pool_size);
  }

  @Override
  protected void exampleConfiguration() {
    example(merge_table_url.key(), "http://host:port/table.jsp?name=namespace:table");
    example(merge_condition.key(), "all");
    example(merge_size_threshold.key(), "100");
    example(merge_rreq_threshold.key(), "0");
    example(runs_interval_sec.key(), "500");
    example(condition_logic.key(), "OR");
    example(hbase_version2.key(), "false");
    example(thread_pool_size.key(), "3");
  }

  Admin admin;
  TableName table;
  long threshold_bytes;
  int read_requests;
  LOGIC logic;
  int round = Constants.UNSET_INT;
  ThreadPoolExecutor pool;

  final Conditions conditions = new Conditions();
  final MergeCondition size_condition = region -> region.getSizeInBytes() < threshold_bytes;
  final MergeCondition rreq_condition = region -> region.readRequests() <= read_requests;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
    logic = (LOGIC) condition_logic.value();
     pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(thread_pool_size.value());
    table = TableName.valueOf(merge_table_url.value().substring(merge_table_url.value().indexOf("=") + 1));


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
      Document doc = Jsoup.connect(merge_table_url.value()).maxBodySize(0).get();
      Element element = doc.getElementById("regionServerDetailsTable");
      TableInfo table_info = new TableInfo(element, hbase_version2.value());
      if (round == Constants.UNSET_INT) {
        // It is determined by first run.
        round = calculateHowManyRuns(table_info);
      }
      for (int i = 0, index_a, index_b; i < table_info.regionNum();) {
        index_a = i++;
        index_b = i++;
        if (index_b >= table_info.regionNum()) {
          break;
        }
        RegionInfo region_A = table_info.getRegionAtIndex(index_a);
        RegionInfo region_B = table_info.getRegionAtIndex(index_b);
        if (conditions.shouldMerge(logic, region_A, region_B)) {
          final HRegionInfo A_region = regions.get(index_a);
          final HRegionInfo B_region = regions.get(index_b);
          pool.submit(() -> {
            LOG.info("Merging region " + Bytes.toStringBinary(A_region.getEncodedNameAsBytes()) + " and " + Bytes.toStringBinary(B_region.getEncodedNameAsBytes()));
            try {
              admin.mergeRegions(A_region.getEncodedNameAsBytes(), B_region.getEncodedNameAsBytes(), false);
            } catch (IOException e) {
              LOG.warning("Error when merging regions " + Bytes.toStringBinary(A_region.getEncodedNameAsBytes()) + " and " + Bytes.toStringBinary(B_region.getEncodedNameAsBytes()) +
                          ", skipping, error: " + e.getMessage());
            }
          });
        }
      }
      BlockingQueue<Runnable> inQueue = pool.getQueue();
      while (!inQueue.isEmpty()) {
        LOG.info("Remaining " + inQueue.size() + " merging tasks");
        LOG.info("Sleeping for " + runs_interval_sec.value() + " seconds to waiting.");
        TimeUnit.SECONDS.sleep(runs_interval_sec.value());
      }
    } while (--round != 0);
    return RETURN_CODE.SUCCESS.code();
  }

  private int calculateHowManyRuns(TableInfo table) {
    int qualified_for_merge = 0;
    for (RegionInfo region : table.getRegions()) {
      qualified_for_merge += conditions.shouldMerge(region) ? 1 : 0;
    }
    int result = (int) (Math.log(qualified_for_merge) / Math.log(2));
    LOG.info("There are " + qualified_for_merge + " regions qualified for merge, and will be approximate " + result + " runs");
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

  enum LOGIC {
    AND, OR
  }

  class Conditions {
    List<MergeCondition> conditions = new ArrayList<>();

    void addCondition(MergeCondition condition) {
      conditions.add(condition);
    }

    public boolean shouldMerge(LOGIC logic, RegionInfo region_A, RegionInfo region_B) {
      for (MergeCondition condition : conditions) {
        switch (logic) {
          case  OR: {
            if (condition.shouldMerge(region_A) || condition.shouldMerge(region_B)) {
              return true;
            }
            break;
          }
          case AND: {
            if (condition.shouldMerge(region_A) && condition.shouldMerge(region_B)) {
              return true;
            }
            break;
          }
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

}
