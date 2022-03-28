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
import org.apache.aries.factory.HandlerFactory;
import org.apache.aries.factory.PutHandlerFactory;
import org.apache.aries.common.Constants;
import org.apache.aries.common.LongParameter;
import org.apache.aries.common.Parameter;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PutWorkload extends BaseWorkload {

  private final Parameter<Long> buffer_size =
      LongParameter.newBuilder(getParameterPrefix() + ".buffer_size").setDefaultValue(Constants.ONE_MB)
                   .setDescription("Buffer size in bytes for batch put").opt();

  private final AtomicLong totalRows = new AtomicLong(0);

  @Override
  public void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(buffer_size);
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(buffer_size.key(), "1024");
  }

  @Override
  protected HandlerFactory initHandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    return new PutHandlerFactory(configuration, parameters);
  }

  @Override
  protected int haveFun() throws Exception {
    super.haveFun();
    LOG.info("Total wrote " + totalRows.get() + " rows in " + running_time.value() + " seconds.");
    LOG.info("Avg " + (double) (totalRows.get()) / running_time.value());
    LOG.info("Existing.");
    return 0;
  }

  @Override protected String getParameterPrefix() {
    return "pw";
  }

}
