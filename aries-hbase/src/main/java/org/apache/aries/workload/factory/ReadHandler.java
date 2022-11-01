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

import com.codahale.metrics.Counter;
import org.apache.aries.workload.factory.HandlerFactory.BaseHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;

public abstract class ReadHandler extends BaseHandler {

  protected static final Counter EMPTY_VALUE = registry.counter("Empty_Value_Read");
  protected static final Counter WRONG_VALUE = registry.counter("Wrong_Value_Read");
  protected static final Counter CORRECT_VALUE = registry.counter("Correct_Value_Read");

  public ReadHandler(Configuration conf, TableName table) throws IOException {
    super(conf, table);
  }

}
