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

package org.apache.aries.workload.factory;

import org.apache.aries.workload.MixWorkload.MODE;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.Parameter;
import org.apache.aries.workload.factory.GetHandlerFactory.GetHandler;
import org.apache.aries.workload.factory.PutHandlerFactory.PutHandler;
import org.apache.aries.workload.factory.ScanHandlerFactory.ScanHandler;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

public class MixHandlerFactory extends HandlerFactory {

  private final MODE m;
  private int l_handlers;
  private int r_handlers;

  public MixHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters,
                           MODE mode, int left_handlers, int right_handlers) {
    super(configuration, parameters);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(PutHandler.BUFFER_SIZE))          hbase_conf.setLong(PutHandler.BUFFER_SIZE, (long) parameter.value());
      if (parameter.key().contains(PutHandler.VALUE_SIZE))           hbase_conf.setInt(PutHandler.VALUE_SIZE, (int) parameter.value());
      if (parameter.key().contains(ScanHandler.REVERSE_ALLOWED))     hbase_conf.setBoolean(ScanHandler.REVERSE_ALLOWED, (boolean) parameter.value());
      if (parameter.key().contains(ScanHandler.RESULT_VERIFICATION)) hbase_conf.setBoolean(ScanHandler.RESULT_VERIFICATION, (boolean) parameter.value());
      if (parameter.key().contains(GetHandler.RANDOM_OPS))           hbase_conf.setBoolean(GetHandler.RANDOM_OPS, (boolean) parameter.value());
    }

    m = mode;
    l_handlers = left_handlers;
    r_handlers = right_handlers;
  }

  @Override
  public BaseHandler createHandler(TableName table) throws IOException {
    switch (m) {
      case PUT_SCAN: {
        if (l_handlers-- > 0) return new PutHandler(hbase_conf, table);
        if (r_handlers-- > 0) return new ScanHandler(hbase_conf, table);
        break;
      }
      case PUT_GET: {
        if (l_handlers-- > 0) return new PutHandler(hbase_conf, table);
        if (r_handlers-- > 0) return new GetHandler(hbase_conf, table);
      }
      case GET_SCAN: {
        if (l_handlers-- > 0) return new GetHandler(hbase_conf, table);
        if (r_handlers-- > 0) return new ScanHandler(hbase_conf, table);
      }
      default:
        throw new IOException("Unsupported pattern");
    }
    // shouldn't reach here
    return null;
  }

}
