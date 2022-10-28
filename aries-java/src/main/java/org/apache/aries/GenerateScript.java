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
import org.apache.aries.common.FileLineIterator;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.StringParameter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class GenerateScript extends AbstractJavaToy {

  private final Parameter<Integer> parameters_set =
      IntParameter.newBuilder("gs.paramters_set").setRequired()
                  .setDescription("The number of parameters set, no more than 10.").addConstraint(s -> s > 0 && s < 10).opt();
  private final Parameter<Integer> paramters_num_in_set =
      IntParameter.newBuilder("gs.paramters_num_in_set").setRequired()
                  .setDescription("The number of parameters in one set").addConstraint(s -> s > 0).opt();
  private final Parameter<String[]> parameters =
      StringArrayParameter.newBuilder("gs.paramters")
                          .setDescription("It should be configured as gs.paramters.$1, gs.paramters.$2 and so on. 1 means it is the first parameter set.").opt();
  private final Parameter<String> script_template =
      StringParameter.newBuilder("gs.script_template").setRequired()
                     .setDescription("A template of a script. It should locate to file path level").opt();
  private final Parameter<Boolean> repeat_as_line =
      BoolParameter.newBuilder("gs.repeat_as_line", true)
                   .setDescription("Whether to parameterize each pattern line, false will parameterize whole script as a block").opt();
  private final Parameter<String> script_output =
      StringParameter.newBuilder("gs.script_output_dir").setRequired()
                     .setDescription("A dir location for generated scripts.").opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(parameters_set);
    requisites.add(paramters_num_in_set);
    requisites.add(parameters);
    requisites.add(script_template);
    requisites.add(repeat_as_line);
    requisites.add(script_output);
  }

  @Override
  protected void exampleConfiguration() {
    example(parameters_set.key(), "1");
    example(parameters.key() + ".$1", "0,1,2,3,4,5,6,7,8,9");
    // Supposing the template in /tmp/clean_disk is
    // rm -rf /disk/$1/data
    example(script_template.key(), "/tmp/clean_disk");
    example(repeat_as_line.key(), "true");
    example(script_output.key(), "/tmp/script");
  }

  private File script_dir;
  private File source_script;
  private ToyConfiguration configuration;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    this.configuration = configuration;
  }

  @Override
  protected void midCheck() {
    script_dir = new File(script_output.value());
    if (!script_dir.exists()) throw new IllegalArgumentException("Directory " + script_output.value() + " doesn't exist");
    if (!script_dir.canExecute() ||
        !script_dir.canWrite()) throw new RuntimeException("Is " + script_output.value() + " executable or writeable?");

    source_script = new File(script_template.value());
    if (!source_script.exists()) throw new IllegalArgumentException("Script template " + script_template.value() + " doesn't exist");
    if (!source_script.canRead()) throw new RuntimeException("Is " + script_template.value() + " readable?");
  }

  @Override
  protected int haveFun() throws Exception {
    return generateScript(readScriptTemplate(),
                          readScriptParamters(),
                          repeat_as_line.value());
  }

  @Override protected String getParameterPrefix() {
    return "gs";
  }

  private List<String> readScriptTemplate() {
    List<String> lines = new ArrayList<>();
    try (FileLineIterator iter = new FileLineIterator(source_script)) {
      while (iter.hasNext()) lines.add(iter.next());
    }
    return lines;
  }

  private Map<Integer, List<String>> readScriptParamters() {
    Map<Integer, List<String>> map = new HashMap<>();
    int p_set;
    String key_prefix = parameters.key() + ".$";
    String key;
    for (int i = 0; i < parameters_set.value(); i++) {
      p_set = i + 1;
      key = key_prefix + p_set;
      if (!configuration.containsKey(key)) {
        throw new IllegalArgumentException("Can find parameter " + key);
      }
      String[] parameters = configuration.getStrings(key, ",");
      if (parameters.length != paramters_num_in_set.value())
        throw new IllegalArgumentException("Number of parameters in " + key + " is not equals to the value of " + paramters_num_in_set.key());
      map.put(p_set, new ArrayList<>(Arrays.asList(parameters)));
    }
    return map;
  }

  private static final String HEADER = "Generated by Aries.";

  private int generateScript(List<String> template_lines, Map<Integer, List<String>> p_sets, boolean repeat_line) throws IOException {
    String name = source_script.getName();
    File script = new File(script_dir, source_script.getName());
    String header = (name.endsWith("sql") ? "-- " : "# ") + HEADER;
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(script))) {
      writer.write(header);
      writer.newLine();
      if (repeat_line) {
        for (String line : template_lines) {
          if (line.contains("$")) {
            for (int i = 0; i < paramters_num_in_set.value(); i++) {
              patternReplace(p_sets, writer, line, i);
            }
          } else {
            writer.newLine();
            writer.write(line);
          }
        }
      } else {
        for (int i = 0; i < paramters_num_in_set.value(); i++) {
          for (String line : template_lines) {
            patternReplace(p_sets, writer, line, i);
          }
        }
      }
      writer.newLine();
    }
    script.setExecutable(true);
    LOG.info("Generated script: " + script);
    return RETURN_CODE.SUCCESS.code();
  }

  private void patternReplace(Map<Integer, List<String>> p_sets, BufferedWriter writer, String line, int index) throws IOException {
    writer.newLine();
    while (line.contains("$")) {
      int set = Character.digit(line.charAt(line.indexOf("$") + 1), 10);
      if (set < 0) break;
      String key_for_replact = "$" + set;
      line = line.replace(key_for_replact, p_sets.get(set).get(index));
    }
    writer.write(line);
  }

  @Override
  protected void destroyToy() throws Exception {
    // Do nothing
  }

}
