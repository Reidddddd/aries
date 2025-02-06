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

public class RegionInfo {

  String name;
  String server;
  String read_requests;
  String write_requests;
  String file_size;
  String file_num;
  String mem_size;
  String locality;
  String start_key;
  String end_key;

  RegionInfo(Element region) {
    Elements column = region.select("td");
    int i = 0;
    name           = column.get(i++).text();
    server         = column.get(i++).text();
    read_requests  = column.get(i++).text();
    write_requests = column.get(i++).text();
    file_size      = column.get(i++).text();
    file_num       = column.get(i++).text();
    mem_size       = column.get(i++).text();
    locality       = column.get(i++).text();
    start_key      = column.get(i++).text();
    end_key        = column.get(i++).text();
  }

  public long getSizeInBytes() {
    long size;
    String num = file_size.split(" ")[0];
    if (file_size.contains("GB")) {
      float gb = Float.parseFloat(num); // get the number
      size = (long) (gb * Constants.ONE_GB);
    } else if (file_size.contains("MB")) {
      float mb = Float.parseFloat(num);
      size = (long) (mb * Constants.ONE_MB);
    } else if (file_size.contains("KB")) {
      float kb = Float.parseFloat(num);
      size = (long) (kb * Constants.ONE_KB);
    } else if (file_size.contains("TB")) {
      float tb = Float.parseFloat(num);
      size = (long) (tb * Constants.ONE_TB);
    } else {
      size = Long.parseLong(num);
    }
    return size;
  }

  public int readRequests() {
    return Integer.parseInt(read_requests.replaceAll(",", ""));
  }

  @Override
  public String toString() {
    return "RegionInfo{" +
        "name='" + name + '\'' +
        ", server='" + server + '\'' +
        ", read_requests='" + read_requests + '\'' +
        ", write_requests='" + write_requests + '\'' +
        ", file_size='" + file_size + '\'' +
        ", file_num='" + file_num + '\'' +
        ", mem_size='" + mem_size + '\'' +
        ", locality='" + locality + '\'' +
        ", start_key='" + start_key + '\'' +
        ", end_key='" + end_key + '\'' +
        '}';
  }

}
