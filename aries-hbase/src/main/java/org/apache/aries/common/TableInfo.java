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

import java.util.ArrayList;
import java.util.List;

public class TableInfo {

  final List<RegionInfo> regions;

  public TableInfo(Element table) {
    this(table, false);
  }

  public TableInfo(Element table, boolean v2) {
    Elements rows = table.select("tr");
    regions = new ArrayList<>(rows.size() - 1); // Skip first title row
    for (int i = 1; i < rows.size(); i++) {
      regions.add(v2 ? new RegionInfoV2(rows.get(i))
                     : new RegionInfo(rows.get(i)));
    }
  }

  public List<RegionInfo> getRegions() {
    return regions;
  }

  public int regionNum() {
    return regions.size();
  }

  public RegionInfo getRegionAtIndex(int i) {
    return regions.get(i);
  }

}
