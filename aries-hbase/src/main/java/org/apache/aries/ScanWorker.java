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

import org.apache.aries.common.BaseHandler;
import org.apache.aries.common.BaseWorker;
import org.apache.aries.common.VALUE_KIND;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

public class ScanWorker extends BaseWorker {

  @Override
  protected BaseHandler createHandler(ToyConfiguration configuration) throws IOException {
    return new ScanHandler(configuration);
  }

  @Override
  protected String getParameterPrefix() {
    return "sw";
  }

  class ScanHandler extends BaseHandler {

    ScanHandler(ToyConfiguration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      try {
        Table target_table = connection.getTable(table);
        while (running) {
          String key = getKey(key_prefix, key_length.value());
          Scan scan = new Scan();
          String k1 = getKey(key_prefix, key_length.value());
          String k2 = getKey(key_prefix, key_length.value());
          Pair<byte[], byte[]> boundaries = getBoundaries(k1, k2);
          scan.addFamily(Bytes.toBytes(family.value()));
          scan.withStartRow(boundaries.getFirst());
          scan.withStopRow(boundaries.getSecond());
          scan.setCacheBlocks(false);
          scan.addColumn(Bytes.toBytes(family.value()), Bytes.toBytes("q"));
          ResultScanner scanner = target_table.getScanner(scan);
          for (Result result = scanner.next(); result != null; result = scanner.next()) {
            if (result.isEmpty()) {
            } else {
              byte[] value = result.getValue(Bytes.toBytes(family.value()), Bytes.toBytes("q"));
              if (kind == VALUE_KIND.FIXED) {
                if (verifiedResult(kind, key, value)) {
                } else {
                }
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warning("Error occured " + e.getMessage());
      } finally {
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
