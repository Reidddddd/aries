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

package org.apache.aries.chaos.action;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class Action implements Callable<Integer> {

  protected static final Logger LOG = Logger.getLogger(Action.class.getName());

  protected Connection connection;

  protected Action() {
  }

  public void init(Configuration configuration, Connection connection) throws IOException {
    LOG.info("Running " + this.getClass().getSimpleName() + "Action");
    this.connection = connection;
  }

}
