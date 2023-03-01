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

import org.apache.aries.common.BoolParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.bookkeeper.client.BKException;

public class DeleteLedgers extends AbstractBookkeeperToy {
  protected final Parameter<String> deleteLedgers = StringParameter.newBuilder("bkdl.ledgers")
      .setRequired()
      .setDescription("The ledgers need to remove.")
      .opt();

  protected final Parameter<Boolean> deleteRange = BoolParameter.newBuilder("bkdl.range", false)
      .setDescription("Whether delete the ledgers in the given range.")
      .opt();

  @Override
  protected String getParameterPrefix() {
    return "bkdl";
  }

  @Override
  protected void exampleConfiguration() {
    example(deleteLedgers.key(), "0,1,2,4");
    example(deleteRange.key(), "false");
  }

  @Override
  protected int haveFun() throws Exception {
    String[] ledgerStrs = deleteLedgers.value().split(",");
    if (deleteRange.value()) {
      if (ledgerStrs.length != 2) {
        throw new IllegalArgumentException("Delete ledgers must be two when using range delete.");
      }
      long bottom = Long.parseLong(ledgerStrs[0]);
      long up = Long.parseLong(ledgerStrs[1]);
      for (long i = bottom; i < up; i++) {
        deleteOneLedger(i);
      }
    } else {
      for (String ledger : ledgerStrs) {
        long id = Long.parseLong(ledger);
        deleteOneLedger(id);
      }
    }
    return 0;
  }

  private void deleteOneLedger(long ledgerId) throws BKException, InterruptedException {
    bookKeeperClient.deleteLedger(ledgerId);
  }
}
