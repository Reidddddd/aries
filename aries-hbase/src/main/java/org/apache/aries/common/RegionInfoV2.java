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

package org.apache.aries.common;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * V2 is based on hbase version 2
 */
public class RegionInfoV2 extends RegionInfo {

  String state;

  RegionInfoV2(Element region) {
    Elements column = region.select("td");
    int i = 0;
    name           = column.get(i++).text();
    server         = column.get(i++).text();
    read_requests  = column.get(i++).text();
    write_requests = column.get(i++).text();
    file_size      = column.get(i++).text();
    file_num       = column.get(i++).text();
    mem_size       = column.get(i++).text();
    start_key      = column.get(i++).text();
    end_key        = column.get(i++).text();
    state          = column.get(i++).text();
  }

  @Override
  public String toString() {
    return "RegionInfoV2{" +
        "name='" + name + '\'' +
        ", server='" + server + '\'' +
        ", read_requests='" + read_requests + '\'' +
        ", write_requests='" + write_requests + '\'' +
        ", file_size='" + file_size + '\'' +
        ", file_num='" + file_num + '\'' +
        ", mem_size='" + mem_size + '\'' +
        ", start_key='" + start_key + '\'' +
        ", end_key='" + end_key + '\'' +
        ", state='" + state + '\'' +
        '}';
  }

}
