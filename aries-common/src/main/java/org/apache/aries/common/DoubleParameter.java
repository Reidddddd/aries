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

public final class DoubleParameter {

  /**
   * A builder for creating double parameter.
   * @param key key name for this parameter
   * @return a double parameter builder
   */
  public static DoubleBuilder newBuilder(String key) {
    return new DoubleBuilder(key);
  }

  private DoubleParameter() {}

  public static class DoubleBuilder extends AbstractTypeBuilderWithValue<Double> {

    private DoubleBuilder(String key) {
      builder.setKey(key).setType(Double.class);
    }

  }

}
