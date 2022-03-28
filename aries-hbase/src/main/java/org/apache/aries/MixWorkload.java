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

import org.apache.aries.common.BaseWorkload;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Constants;
import org.apache.aries.common.DoubleParameter;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.factory.HandlerFactory;
import org.apache.aries.factory.MixHandlerFactory;

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
  private final Parameter<Long> buffer_size =
      LongParameter.newBuilder(getParameterPrefix() + ".buffer_size").setDefaultValue(Constants.ONE_MB)
                   .setDescription("Buffer size in bytes for batch put").opt();
  private final Parameter<Boolean> reverse_scan =
      BoolParameter.newBuilder(getParameterPrefix() + ".reverse_scan_allowed", false)
                   .setDescription("If set true, there will be some reverse scan").opt();
  private final Parameter<Boolean> result_verification =
      BoolParameter.newBuilder(getParameterPrefix() + ".result_verification", false)
                   .setDescription("If set true, there will be verification for the returned results").opt();

  public enum MODE {
    PUT_SCAN
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
    requisites.add(reverse_scan);
    requisites.add(result_verification);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(work_load.key(), "PUT_SCAN");
    example(work_load_ratio.key(), "0.5");
    example(buffer_size.key(), "1024");
    example(reverse_scan.key(), "false");
    example(result_verification.key(), "true");
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
