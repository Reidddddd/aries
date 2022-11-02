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


package org.apache.aries.chaos;

import org.apache.aries.common.RETURN_CODE;

public class ForceBalanceRegions extends Action {

  public ForceBalanceRegions() {}

  @Override
  public Integer call() throws Exception {
    boolean ran = connection.getAdmin().balancer(true);
    if (ran) {
      LOG.info("Force balancer to run successfully");
    } else {
      LOG.warning("Unable to force balancer to run, please check master log for details");
    }
    return RETURN_CODE.SUCCESS.code();
  }

}
