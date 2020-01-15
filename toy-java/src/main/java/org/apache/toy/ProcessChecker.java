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

package org.apache.toy;

import org.apache.toy.common.FileLineIterator;
import org.apache.toy.common.HelpPrinter;
import org.apache.toy.common.Parameter;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ProcessChecker extends AbstractJavaToy {

  private final Parameter<String> process_check_file = Parameter.newBuilder().setKey("process_check_file").setType(String.class)
                                                                .setRequired(true).setDescription("file contains process information to checked")
                                                                .opt();
  private final Parameter<String> section_delimiter = Parameter.newBuilder().setKey("section_delimiter").setType(String.class)
                                                               .setRequired(true).setDescription("section separator")
                                                               .opt();
  private final Parameter<String[]> normal_processes = Parameter.newBuilder().setKey("normal_processes").setType(String[].class)
                                                                .setRequired(true).setDescription("normal processes")
                                                                .opt();

  public ProcessChecker() {}

  @Override
  public void init() {
    parameters.add(process_check_file);
    parameters.add(section_delimiter);
    parameters.add(normal_processes);
  }

  @Override
  public int howToPlay(PrintStream out) {
    HelpPrinter.printUsage(out, this.getClass(), parameters);
    return RETURN_CODE.HELP.code();
  }

  @Override
  public int haveFun() throws Exception {
    try (FileLineIterator fli = new FileLineIterator(process_check_file.value())) {
      boolean first_section = true;
      MachineProcesses mp = new MachineProcesses(Arrays.asList(normal_processes.value()));

      while (fli.hasNext()) {
        String line = fli.next();

        // Deal with secion header
        if (line.contains(section_delimiter.value())) {
          if (first_section) {
            first_section = false;
            mp.hostname = line.split(section_delimiter.value())[1].trim();
            continue;
          } else if (mp.abnormal()) {
            mp.listAbnormalOnly();
          }
          mp.init();
          mp.hostname = line.split(section_delimiter.value())[1].trim();
          continue;
        }

        // Deal with processes lines
        mp.judgeProcess(line);
      }
    }
    return RETURN_CODE.SUCCESS.code();
  }

  private class MachineProcesses {
    String hostname = "";
    Map<String, Boolean> normal = new HashMap<>();
    Set<String> others = new HashSet<>();

    MachineProcesses(List<String> processes) {
      processes.forEach(p -> normal.put(p.toLowerCase(), Boolean.FALSE));
      init();
    }

    void judgeProcess(String process) {
      String p = process.split(" ")[1].toLowerCase();
      if (normal.containsKey(p)) normal.put(p, Boolean.TRUE);
      else others.add(p);
    }

    boolean abnormal() {
      return !others.isEmpty() || normal.values().contains(Boolean.FALSE);
    }

    void init() {
      hostname = "";
      others.clear();
      normal.keySet().forEach(k -> normal.put(k, Boolean.FALSE));
    }

    void listAbnormalOnly() {
      StringBuilder b = new StringBuilder("Host: " + hostname + " contains");
      if (others.isEmpty()) {
        b.append(" no abnormal processes.");
      } else {
        others.forEach(s -> b.append(" " + s + ","));
      }
      normal.forEach((k, v) -> {
        if (!v) {
          b.append(" lacks " + k);
        }
      });
      System.out.println(b.toString());
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder("Host: " + hostname + " ");
      normal.forEach((k, v) -> {
        if (!v) b.append("doesn't contain " + k + " ");
        else b.append("contains " + k);
      });
      others.forEach(s -> b.append(" also contains " + s));
      return b.toString();
    }
  }

}