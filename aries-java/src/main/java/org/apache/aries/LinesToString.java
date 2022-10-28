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

import org.apache.aries.common.FileLineIterator;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringParameter;

import java.io.File;
import java.util.List;

public class LinesToString extends AbstractJavaToy {

  private final Parameter<String> lines =
      StringParameter.newBuilder("lts.lines").setRequired().setDescription("A file contains lines to be dealt with").opt();
  private final Parameter<String> separtor =
      StringParameter.newBuilder("lts.separtor").setRequired().setDescription("Separator between line").opt();
  private final Parameter<String> decorator =
      StringParameter.newBuilder("lts.decorator").setDefaultValue("").setDescription("Decorator for a line, e.g., 'line'.").opt();
  private final Parameter<String> wrappers =
      StringParameter.newBuilder("lts.wrappers").setDefaultValue("").setDescription("Wrapper for the generated string, e.g., ['a','b','c'], [] are the wrappers.").opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(lines);
    requisites.add(separtor);
    requisites.add(decorator);
    requisites.add(wrappers);
  }

  @Override
  protected void exampleConfiguration() {
    example(lines.key(), "/path/to/file");
    example(separtor.key(), ",");
    example(decorator.key(), "'");
    example(wrappers.key(), "[]");
  }

  private File file;

  @Override
  protected void midCheck() {
    file = new File(lines.value());
    if (!file.exists())  throw new RuntimeException(file + " doesn't exist");
    if (!file.canRead()) throw new RuntimeException(file + " can't be read");

    if (wrappers.value().length() != 0 && wrappers.value().length() != 2) throw new RuntimeException("wrappers size can only be 2 or 0");
  }

  private char left_wrapper;
  private char rght_wrapper;

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    if (wrappers.value().length() == 0) {
      left_wrapper = '\0';
      rght_wrapper = '\0';
    } else if (wrappers.value().length() == 2) {
      left_wrapper = wrappers.value().charAt(0);
      rght_wrapper = wrappers.value().charAt(1);
    }
  }

  @Override
  protected int haveFun() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append(left_wrapper);
    try (FileLineIterator iter = new FileLineIterator(file)) {
      while (iter.hasNext()) {
        builder.append(decorator.value())
               .append(iter.next())
               .append(decorator.value())
               .append(separtor.value());
      }
    }
    String near_the_result = builder.toString();
    String result = near_the_result.substring(0, near_the_result.lastIndexOf(separtor.value())) + rght_wrapper;
    LOG.info("Processed outcome: " + result);
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  protected void destroyToy() throws Exception {
  }

  @Override protected String getParameterPrefix() {
    return "lts";
  }

}
