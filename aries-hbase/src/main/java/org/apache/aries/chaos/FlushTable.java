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

import org.apache.hadoop.hbase.TableName;

public class FlushTable extends TableBase {

  public FlushTable() {}

  @Override
  protected void perform(TableName table) throws Exception {
    admin.flush(table);
  }

  @Override
  protected void prePerform(TableName table) throws Exception {
    super.prePerform(table);
    LOG.info("Start flushing " + table);
  }

  @Override
  protected void postPerform(TableName table) throws Exception {
    super.postPerform(table);
    LOG.info("Finish flushing " + table + " in " + getDuration() + " seconds");
  }

}
