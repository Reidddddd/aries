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

public enum RETURN_CODE {

  HELP(-2),
  SUCCESS(0),
  FAILURE(1);

  private int return_code;

  RETURN_CODE(int return_code) {
    this.return_code = return_code;
  }

  public int code() {
    return return_code;
  }

}
