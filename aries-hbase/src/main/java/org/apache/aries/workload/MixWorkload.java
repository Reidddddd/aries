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

package org.apache.aries.workload;

import org.apache.aries.ToyConfiguration;
import org.apache.aries.workload.common.BaseWorkload;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Constants;
import org.apache.aries.common.DoubleParameter;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.ToyUtils;
import org.apache.aries.workload.factory.GetHandlerFactory.GetHandler;
import org.apache.aries.workload.factory.HandlerFactory;
import org.apache.aries.workload.factory.MixHandlerFactory;
import org.apache.aries.workload.factory.PutHandlerFactory.PutHandler;
import org.apache.aries.workload.factory.ScanHandlerFactory.ScanHandler;

import java.util.List;

public class MixWorkload extends BaseWorkload {

  private final Parameter<Enum> work_load =
      EnumParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + "work_load", MODE.PUT_SCAN, MODE.class)
                   .setRequired()
                   .setDescription("Mix work load type. There're three types: PUT_SCAN, PUT_GET and GET_SCAN.")
                   .opt();
  private final Parameter<Double> work_load_ratio =
      DoubleParameter.newBuilder(getParameterPrefix() + ".ratio")
                     .setDefaultValue(0.5)
                     .addConstraint(v -> v > 0 && v < 1)
                     .setDescription("Ratio between operations. It should be between 0 to 1.")
                     .opt();
  private final Parameter<Long> buffer_size =
      LongParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + PutHandler.BUFFER_SIZE)
                   .setDefaultValue(Constants.ONE_MB)
                   .setDescription("Buffer size in bytes for batch put")
                   .opt();
  private final Parameter<Integer> value_size_in_bytes =
      IntParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + PutHandler.VALUE_SIZE)
                  .setDescription("Size of each value being put, in byte unit")
                  .opt();
  private final Parameter<Boolean> reverse_scan =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + ScanHandler.REVERSE_ALLOWED, false)
                   .setDescription("If set true, there will be some reverse scan")
                   .opt();
  private final Parameter<Boolean> result_verification =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + ScanHandler.RESULT_VERIFICATION, false)
                   .setDescription("If set true, there will be verification for the returned results")
                   .opt();
  private final Parameter<Boolean> random_ops =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + GetHandler.RANDOM_OPS, true)
                   .setDescription("It must be set false, if using SEQ key mode")
                   .opt();

  public enum MODE {
    PUT_SCAN, PUT_GET, GET_SCAN
  }
  private MODE mode;
  private int left, right;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
       mode = (MODE) work_load.value();
    int num = num_connections.value();
       left = (int) (num * work_load_ratio.value());
      right = num - left;
    super.buildToy(configuration);
  }

  @Override
  public void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(work_load);
    requisites.add(work_load_ratio);

    requisites.add(buffer_size);
    requisites.add(value_size_in_bytes);

    requisites.add(reverse_scan);
    requisites.add(result_verification);

    requisites.add(random_ops);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(work_load.key(), "PUT_SCAN");
    example(work_load_ratio.key(), "0.5");
    example(buffer_size.key(), "1024");
    example(reverse_scan.key(), "false");
    example(result_verification.key(), "true");
    example(random_ops.key(), "true");
  }

  @Override
  protected HandlerFactory initHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    return new MixHandlerFactory(configuration, parameters, mode, left, right);
  }

  @Override
  protected String getParameterPrefix() {
    return "mw";
  }

}
