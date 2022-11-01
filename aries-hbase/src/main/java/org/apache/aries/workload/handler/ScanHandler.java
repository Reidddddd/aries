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

package org.apache.aries.workload.handler;

import com.codahale.metrics.Meter;
import org.apache.aries.workload.common.VALUE_KIND;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Random;

public class ScanHandler extends ReadHandler {

  public static final String REVERSE_ALLOWED = "reverse_scan_allowed";
  public static final String RESULT_VERIFICATION = "result_verification";
  public static final String METRICS_INDIVIDUAL_SCAN = "metrics_each_scan";
  final Random random = new Random();

  private Meter rows;

  public ScanHandler(Configuration conf, TableName table) throws IOException {
    super(conf, table);
  }

  @Override
  public void run() {
    String thread_name = Thread.currentThread().getName();
    if (hbase_conf.getBoolean(METRICS_INDIVIDUAL_SCAN, false)) {
      rows = registry.meter(thread_name + "_scan_rows");
    }
    try {
      Table target_table = connection.getTable(getTable());
      while (!isInterrupted()) {
        long start_time = System.nanoTime();
        String k1 = getKey(key_kind, key_length, true);
        String k2 = getKey(key_kind, key_length, true);
        Pair<byte[], byte[]> boundaries = getBoundaries(k1, k2);
        Scan scan = new Scan();
        scan.withStartRow(boundaries.getFirst());
        scan.withStopRow(boundaries.getSecond());
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("q"));
        if (hbase_conf.getBoolean(REVERSE_ALLOWED, false)) {
          scan.setReversed(random.nextInt(2) != 0);
        }

        ResultScanner scanner = target_table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          if (result.isEmpty()) {
            EMPTY_VALUE.inc();
          } else {
            byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes("q"));
            if (rows != null) rows.mark();
            READ_ROW.mark();
            if (hbase_conf.getBoolean(RESULT_VERIFICATION, false)) {
              if (value_kind == VALUE_KIND.FIXED) {
                if (!verifiedResult(value_kind, Bytes.toString(result.getRow()), value)) {
                  WRONG_VALUE.inc();
                } else {
                  CORRECT_VALUE.inc();
                }
              }
            }
          }
        }
        long stop_time = System.nanoTime();
        if (rows != null) {
          LOG.info(thread_name + " scans " + rows.getCount() + " rows in " + (stop_time - start_time) + " ns");
        }
      }
    } catch (Exception e) {
      LOG.warning("Error occured!");
      e.printStackTrace();
    }
  }

  private Pair<byte[], byte[]> getBoundaries(String k1, String k2) {
    Pair<byte[], byte[]> boundaries = new Pair<>();
    int res = Bytes.compareTo(Bytes.toBytes(k1), Bytes.toBytes(k2));
    if (res < 0) {
      boundaries.setFirst(Bytes.toBytes(k1));
      boundaries.setSecond(Bytes.toBytes(k2));
    } else {
      boundaries.setFirst(Bytes.toBytes(k2));
      boundaries.setSecond(Bytes.toBytes(k1));
    }
    return boundaries;
  }

}
