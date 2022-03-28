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

import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.KEY_PREFIX;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.VALUE_KIND;
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
    }
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    return new ScanHandler(hbase_conf, table);
  }

  public static class ScanHandler extends BaseHandler {

    public static final String REVERSE_ALLOWED = "reverse_scan_allowed";
    public static final String RESULT_VERIFICATION = "result_verification";
    final Random random = new Random();

    ScanHandler(Configuration conf, TableName table) throws IOException {
      super(conf, table);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(getTable());
        while (!isInterrupted()) {
          String key = getKey(key_kind, key_length);
          String k1 = getKey(key_kind, key_length);
          String k2 = getKey(key_kind, key_length);
          Pair<byte[], byte[]> boundaries = getBoundaries(k1, k2);

          Scan scan = new Scan();
          scan.withStartRow(boundaries.getFirst());
          scan.withStopRow(boundaries.getSecond());
          scan.setCacheBlocks(false);
          scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("q"));
          if (hbase_conf.getBoolean(REVERSE_ALLOWED, false)) {
            scan.setReversed(random.nextInt(2) != 0);
          }
          ResultScanner scanner = target_table.getScanner(scan);
          for (Result result = scanner.next(); result != null; result = scanner.next()) {
            if (result.isEmpty()) {
            } else {
              byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes("q"));
              if (hbase_conf.getBoolean(RESULT_VERIFICATION, false)) {
                if (value_kind == VALUE_KIND.FIXED) {
                  if (verifiedResult(value_kind, key, value)) {
                  } else {
                  }
                }
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warning("Error occured " + e.getMessage());
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
