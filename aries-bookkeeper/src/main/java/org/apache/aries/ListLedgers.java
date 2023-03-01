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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.bouncycastle.util.Strings;

public class ListLedgers extends AbstractBookkeeperToy {
  private static final String templateStr = "LedgerId: %d, Size: %d";

  protected final Parameter<String> ledgers = StringParameter.newBuilder("bk.ledgers")
      .setDefaultValue("")
      .setDescription("The specific ledgers you want to list, separated with ,")
      .opt();

  protected List<Long> ledgerList;

  @Override
  protected String getParameterPrefix() {
    return "bk";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(ledgers);
  }

  @Override
  protected void exampleConfiguration() {
    example(zkServers.key(), "localhost:2181");
    example(ledgerPath.key(), "/ledgers");
    example(ledgers.key(), "0,1,2");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    ledgerList = new ArrayList<>();
    String[] ids = Strings.split(ledgers.value(), ',');
    Arrays.stream(ids).forEach(id -> {
      if (!id.equals("")) {
        ledgerList.add(Long.parseLong(id));
      }
    });
  }

  @Override
  protected int haveFun() throws Exception {
    if (ledgerList.isEmpty()) {
      bookKeeperAdmin.listLedgers().forEach(ledgerList::add);
    }
    long sum = 0;
    for (long ledger : ledgerList) {
      long size = bookKeeperClient.getLedgerMetadata(ledger).get().getLength();
      sum += size;
      LOG.info(String.format(templateStr, ledger, size));
    }
    LOG.info("The ledgers total size: " + sum);
    return 0;
  }
}
