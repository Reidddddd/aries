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

import org.apache.aries.annotation.Nullable;
import org.apache.aries.common.Constants;

import java.util.Optional;

public final class ToyParameters {

  private boolean need_help;
  private String toy_name;
  private String conf_dir;
  private String package_name = "org.apache.aries.";

  private ToyParameters(boolean need_help, String toy_name, String conf_dir) {
    this.need_help = need_help;
    this.toy_name = toy_name;
    this.conf_dir =  conf_dir;
  }

  public boolean needHelp() {
    return need_help;
  }

  public String getToyName() {
    return package_name + toy_name;
  }

  public String getSimpleToyName() {
    return toy_name;
  }

  public String getConfDirectory() {
    return conf_dir;
  }

  private static final String USAGE =
      "Usage: aries --kind [java|hbase|hdfs|kafka|phoenix]\n" +
      "             --toy  [name of toy]\n" +
      "             --conf_dir [configuration's directory]\n" +
      "             --help (this will print help message of the toy)";

  public static ToyParameters parse(@Nullable String[] main_args) {
    Optional<String[]> optional_args = Optional.ofNullable(main_args);
    if (!optional_args.isPresent()) {
      throw new IllegalArgumentException("Found no argument");
    }

    String toy_name = Constants.UNSET_STRING;
    String conf_dir = Constants.UNSET_STRING;
    boolean need_help = false;
    String[] args = optional_args.get();
    for (int i = 0; i < args.length; i++) {
           if (args[i].equals("--help")) need_help = true;
      else if (args[i].equals("--toy")) {
           if (++i == args.length) {
             System.out.println(USAGE);
             throw new IllegalArgumentException("Missing value for --toy");
           }
           toy_name = args[i];
      }
      else if (args[i].equals("--conf_dir")) {
           if (++i == args.length) {
             System.out.println(USAGE);
             throw new IllegalArgumentException("Missing value for --conf_dir");
           }
           conf_dir = args[i];
      }
    }

    if (toy_name.equals(Constants.UNSET_STRING)) {
      System.out.println(USAGE);
      throw new IllegalArgumentException("Missing --toy argument");
    }
    if (conf_dir.equals(Constants.UNSET_STRING)) {
      System.out.println(USAGE);
      throw new IllegalArgumentException("Missing --conf_dir argument");
    }
    return new ToyParameters(need_help, toy_name, conf_dir);
  }

}
