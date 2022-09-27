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

package org.apache.aries.factory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.KEY_PREFIX;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.VALUE_KIND;
import org.apache.aries.handler.ReadHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class ScanHandlerFactory extends HandlerFactory {

  public ScanHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    super(configuration, parameters);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(ScanHandler.REVERSE_ALLOWED))     hbase_conf.setBoolean(ScanHandler.REVERSE_ALLOWED, (Boolean) parameter.value());
      if (parameter.key().contains(ScanHandler.RESULT_VERIFICATION)) hbase_conf.setBoolean(ScanHandler.RESULT_VERIFICATION, (Boolean) parameter.value());
      if (parameter.key().contains(ScanHandler.METRICS_INDIVIDUAL_SCAN)) hbase_conf.setBoolean(ScanHandler.METRICS_INDIVIDUAL_SCAN, (Boolean) parameter.value());
    }
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    return new ScanHandler(hbase_conf, table);
  }

  public static class ScanHandler extends ReadHandler {

    public static final String REVERSE_ALLOWED = "reverse_scan_allowed";
    public static final String RESULT_VERIFICATION = "result_verification";
    public static final String METRICS_INDIVIDUAL_SCAN = "metrics_each_scan";
    final Random random = new Random();

    private Meter rows;

    ScanHandler(Configuration conf, TableName table) throws IOException {
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

}
