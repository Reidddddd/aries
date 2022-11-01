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

import org.apache.aries.workload.common.BaseWorkload;
import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.ToyUtils;
import org.apache.aries.workload.factory.GetHandlerFactory;
import org.apache.aries.workload.factory.HandlerFactory;
import org.apache.aries.workload.handler.GetHandler;

import java.util.List;

public class GetWorkload extends BaseWorkload {

  private final Parameter<Boolean> random_ops =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + GetHandler.RANDOM_OPS, true)
                   .setDescription("It must be set false, if using SEQ key mode")
                   .opt();
  private final Parameter<Boolean> result_verification =
      BoolParameter.newBuilder(getParameterPrefix() + ToyUtils.PARAMETER_SEPARATOR + GetHandler.RESULT_VERIFICATION, false)
                   .setDescription("If set true, there will be verification for the returned results")
                   .opt();

  @Override
  public void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(random_ops);
    requisites.add(result_verification);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(random_ops.key(), "true");
    example(result_verification.key(), "false");
  }

  @Override
  protected HandlerFactory initHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    return new GetHandlerFactory(configuration, parameters);
  }

  @Override protected String getParameterPrefix() {
    return "gw";
  }

}
