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
import org.apache.aries.common.Parameter;
import org.apache.aries.factory.HandlerFactory;
import org.apache.aries.factory.ScanHandlerFactory;

import java.util.List;

public class ScanWorkload extends BaseWorkload {

  private final Parameter<Boolean> reverse_scan =
      BoolParameter.newBuilder(getParameterPrefix() + ".reverse_scan_allowed", false)
                   .setDescription("If set true, there will be some reverse scan").opt();
  private final Parameter<Boolean> result_verification =
      BoolParameter.newBuilder(getParameterPrefix() + ".result_verification", false)
                   .setDescription("If set true, there will be verification for the returned results").opt();
  private final Parameter<Boolean> metrics_each_scan =
      BoolParameter.newBuilder(getParameterPrefix() + ".metrics_each_scan", false)
                   .setDescription("If set true, there will a rows metrics for each thread's scan").opt();

  @Override
  public void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(reverse_scan);
    requisites.add(result_verification);
    requisites.add(metrics_each_scan);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(reverse_scan.key(), "false");
    example(result_verification.key(), "false");
    example(metrics_each_scan.key(), "false");
  }

  @Override
  protected HandlerFactory initHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    return new ScanHandlerFactory(configuration, parameters);
  }

  @Override
  protected String getParameterPrefix() {
    return "sw";
  }

}
