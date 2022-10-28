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

import org.apache.aries.common.HelpPrinter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * An abstract toy.
 */
public abstract class AbstractToy implements Toy {

  protected static final Logger LOG = Logger.getLogger(AbstractToy.class.getName());

  @SuppressWarnings("rawtypes")
  private final List<Parameter> parameters = new LinkedList<>();

  @Override
  public final void init() {
    requisite(parameters);
  }

  @Override
  public final int howToPlay() {
    HelpPrinter.printUsage(this.getClass(), parameters);
    LOG.info("Example configurations for " + this.getClass().getSimpleName() + ":");
    exampleConfiguration();
    return RETURN_CODE.HELP.code();
  }

  @Override
  public final int play(String dir_of_conf_file) throws Exception {
    ToyConfiguration toy_conf = ToyConfiguration.create(dir_of_conf_file);
    preCheck(toy_conf, parameters);
    midCheck();
    buildToy(toy_conf);
    printParameters(toy_conf, getParameterPrefix());
    try {
      return haveFun();
    } finally {
      destroyToy();
    }
  }

  protected List<Parameter> getParameters() {
    return parameters;
  }

  protected abstract String getParameterPrefix();

  protected void printParameters(ToyConfiguration toy_conf, String parameter_prefix) {
    LOG.info("Parameters for " +  this.getClass().getSimpleName() + " are:");
    Properties properties = toy_conf.getProperties();
    Set<String> keys = properties.keySet().stream().map(String.class::cast).filter(k -> k.startsWith(parameter_prefix)).collect(Collectors.toSet());
    Map<Parameter, Boolean> printed = new HashMap<>();
    for (Parameter parameter : parameters) {
      printed.put(parameter, false);
    }
    for (Parameter parameter : parameters) {
      for (String key : keys) {
        if (key.equals(parameter.key())) {
          example(parameter.key(), parameter.valueInString());
          printed.put(parameter, true);
        } else if (key.startsWith(parameter.key())) {
          example(key, (String) properties.get(key));
          printed.put(parameter, true);
        }
      }
    }
    for (Map.Entry<Parameter, Boolean> entry : printed.entrySet()) {
      if (!entry.getValue()) {
        example(entry.getKey().key(), String.valueOf(entry.getKey().defvalue()));
      }
    }
  }

  /**
   * Please add all user-defined parameters in this method.
   * @param requisites parameters to be added to
   */
  protected abstract void requisite(@SuppressWarnings("rawtypes") List<Parameter> requisites);

  /**
   * It prints out an example configuration for a toy.
   */
  protected abstract void exampleConfiguration();

  protected void example(String key, String value) {
    LOG.info(key + "=" + value);
  }

  /**
   * This method is used for checking parameters needed for playing toy. Toy should throw IllegalArgumentException
   * if any required parameter is not set.
   * @param configuration toy configuration
   * @param requisites requisites to be checked
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected void preCheck(ToyConfiguration configuration, List<Parameter> requisites) {
    for (Parameter p : requisites) {
      if (!configuration.containsKey(p.key())) {
        if (p.required()) {
          howToPlay();
          throw new IllegalArgumentException(p.key() + " is not set");
        }
        continue;
      }
           if (p.type().equals(String.class))   p.checkAndSet(configuration.get(p.key()));
      else if (p.type().equals(String[].class)) p.checkAndSet(configuration.getStrings(p.key(), ","));
      else if (p.type().isEnum())               p.checkAndSet(configuration.getEnum(p.key(), (Enum)p.value()));
      else if (p.type().equals(Integer.class))  p.checkAndSet(configuration.getInt(p.key()));
      else if (p.type().equals(Boolean.class))  p.checkAndSet(configuration.getBoolean(p.key(), (Boolean)p.value()));
      else if (p.type().equals(Short.class))    p.checkAndSet(configuration.getShort(p.key()));
      else if (p.type().equals(Long.class))     p.checkAndSet(configuration.getLong(p.key()));
      else if (p.type().equals(Double.class))   p.checkAndSet(configuration.getDouble(p.key()));
      else if (p.type().equals(Float.class))    p.checkAndSet(configuration.getFloat(p.key()));
    }
  }

  /**
   * This method is called after preCheck, it may be used in specific toy to do self parameter check.
   * Every parameter should have valid value.
   */
  protected void midCheck() {
  }

  /**
   * Build up toys.
   * @param configuration toy configuration
   * @throws Exception exception if any
   */
  protected abstract void buildToy(ToyConfiguration configuration) throws Exception;

  /**
   * Actual toy playing.
   * @return {@link RETURN_CODE#SUCCESS} for good play, {@link RETURN_CODE#FAILURE} for bad play
   * @throws Exception throw exception when playing toy if any
   */
  protected abstract int haveFun() throws Exception;

  /**
   * After toy played, close resources if any.
   * @throws Exception throw exception if any
   */
  protected abstract void destroyToy() throws Exception;

}
