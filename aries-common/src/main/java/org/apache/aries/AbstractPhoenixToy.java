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

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;


public abstract class AbstractPhoenixToy extends AbstractToy {

  private static final String PHOENIX_URL = "phoenix.connection.url";

  protected PhoenixDriver driver = new PhoenixDriver();
  protected PhoenixConnection connection;

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    connection = (PhoenixConnection) driver.connect(configuration.get(PHOENIX_URL), configuration.getProperties());
  }

  @Override protected void destroyToy() throws Exception {
    connection.close();
    driver.close();
  }

}
