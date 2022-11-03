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

public class DumpClusterStatus extends Action {

  public DumpClusterStatus() {}

  @Override
  public Integer call() throws Exception {
    try {
      ClusterStatus status = connection.getAdmin().getClusterStatus();
      LOG.info("Dumping cluster status");
      LOG.info("Cluster status\n" + status);
    } catch (Throwable t) {
      LOG.warning(ToyUtils.buildError(t));
      return RETURN_CODE.FAILURE.code();
    }
    return RETURN_CODE.SUCCESS.code();
  }

}
