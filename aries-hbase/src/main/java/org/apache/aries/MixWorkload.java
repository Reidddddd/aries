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
import org.apache.aries.common.BaseWorkload;
import org.apache.aries.common.DoubleParameter;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.Parameter;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

public class MixWorkload extends BaseWorkload {

  private final Parameter<Enum> work_load =
      EnumParameter.newBuilder(getParameterPrefix() + ".work_load", MODE.PUT_SCAN, MODE.class).setRequired()
                   .setDescription("Mix work load type. There're three types: PUT_SCAN, PUT_GET and GET_SCAN.")
                   .opt();
  private final Parameter<Double> work_load_ratio =
      DoubleParameter.newBuilder(getParameterPrefix() + ".ratio").setDefaultValue(0.5)
                     .addConstraint(v -> v > 0 && v < 1)
                     .setDescription("Ratio between operations. It should be between 0 to 1.").opt();

  enum MODE {
    PUT_SCAN, PUT_GET, GET_SCAN
  }
  private MODE mode;
  private int left, right;

  private GetWorkload get_work_load = new GetWorkload();
  private PutWorkload put_work_load = new PutWorkload();
  private ScanWorkload scan_work_load = new ScanWorkload();

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    mode = (MODE) work_load.value();
    int num = num_connections.value();
    left = (int) (num * work_load_ratio.value());
    right = num - left;
    super.buildToy(configuration);
  }

  @Override
  protected BaseHandler createHandler(ToyConfiguration configuration, TableName table) throws IOException {
    BaseHandler handler = null;
    switch (mode) {
      case PUT_GET: {
        if (left-- > 0) {
          handler = put_work_load.createHandler(configuration, table);
        } else if (right-- > 0) {
          handler = get_work_load.createHandler(configuration, table);
        }
        break;
      }
      case PUT_SCAN: {
        if (left-- > 0) {
          handler = put_work_load.createHandler(configuration, table);
        } else if (right-- > 0) {
          handler = scan_work_load.createHandler(configuration, table);
        }
        break;
      }
      case GET_SCAN: {
        if (left-- > 0) {
          handler = get_work_load.createHandler(configuration, table);
        } else if (right-- > 0) {
          handler = scan_work_load.createHandler(configuration, table);
        }
        break;
      }
      default:
        throw new IOException("Should not reach here");
    }
    return handler;
  }

  @Override
  public void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(work_load);
    requisites.add(work_load_ratio);
    get_work_load.requisite(requisites);
    put_work_load.requisite(requisites);
    scan_work_load.requisite(requisites);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(work_load.key(), "PUT_SCAN");
    example(work_load_ratio.key(), "0.5");
  }

  @Override
  protected String getParameterPrefix() {
    return "mw";
  }

}
