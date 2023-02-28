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

import java.util.List;

import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;

public abstract class AbstractBookkeeperToy extends AbstractToy {
  protected final Parameter<String> zkServers = StringParameter.newBuilder("bk.zk.servers")
      .setRequired()
      .setDescription("zkServers where the bk cluster located.")
      .opt();
  protected final Parameter<String> ledgerPath = StringParameter.newBuilder("bk.ledgers.path")
      .setRequired()
      .setDescription("ZK node where store the meta data of ledgers.")
      .opt();

  protected BookKeeperAdmin bookKeeperAdmin;
  protected BookKeeper bookKeeperClient;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    bookKeeperAdmin = new BookKeeperAdmin(zkServers.value());
    bookKeeperClient = new BookKeeper(zkServers.value());
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(zkServers);
    requisites.add(ledgerPath);
  }
}
