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
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringParameter;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;

public abstract class AbstractBenchmarkToy extends AbstractToy {

  private final Parameter<Integer> warmup_iterations =
      IntParameter.newBuilder("bm.warmup_iterations").setDefaultValue(5).setDescription("Iterations for warm up").opt();
  private final Parameter<Integer> forks =
      IntParameter.newBuilder("bm.forks").setDefaultValue(1).setDescription("Number of instance forked").opt();
  private final Parameter<Integer> threads =
      IntParameter.newBuilder("bm.threads").setDefaultValue(1).setDescription("Number of threads for benchmark").opt();
  private final Parameter<Enum> mode =
      EnumParameter.newBuilder("bm.mode", Mode.Throughput, Mode.class).setDescription("Benchmark mode, options are: Throughput,AverageTime,SampleTime,SingleShotTime,All").opt();
  private final Parameter<Integer> measure_iterations =
      IntParameter.newBuilder("bm.measure_iterations").setDefaultValue(5).setDescription("Number of iterations after warmup").opt();
  private final Parameter<String> warmup_time =
      StringParameter.newBuilder("bm.warmup_time").setDefaultValue("10s").setDescription("Time for each warmup iteration").opt();
  private final Parameter<String> measure_time =
      StringParameter.newBuilder("bm.measure_time").setDefaultValue("10s").setDescription("Time for each execution iteration").opt();
  private final Parameter<Boolean> gc_profiler =
      BoolParameter.newBuilder("bm.gc_profiler", false).setDescription("If turn on gc profiler, false by default").opt();

  @Override
  protected String getParameterPrefix() {
    return "bm";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(forks);
    requisites.add(threads);
    requisites.add(mode);
    requisites.add(warmup_iterations);
    requisites.add(warmup_time);
    requisites.add(measure_iterations);
    requisites.add(measure_time);
    requisites.add(gc_profiler);
  }

  @Override
  protected void exampleConfiguration() {
    example(forks.key(), "1");
    example(threads.key(), "1");
    example(mode.key(), "thrpt");
    example(warmup_iterations.key(), "5");
    example(warmup_time.key(), "10s");
    example(measure_iterations.key(), "5");
    example(measure_time.key(), "10s");
    example(gc_profiler.key(), "true");
  }

  TimeValue warmup_t = TimeValue.seconds(10);
  TimeValue measure_t = TimeValue.seconds(10);

  @Override
  final protected void buildToy(ToyConfiguration configuration) throws Exception {
    warmup_t = toTimeValue(warmup_time.value());
    measure_t = toTimeValue(measure_time.value());
  }

  private TimeValue toTimeValue(String time_str) {
    int len = time_str.length();
         if (time_str.endsWith("d"))  return TimeValue.days(Long.parseLong(time_str.substring(0, len - 1)));
    else if (time_str.endsWith("h"))  return TimeValue.hours(Long.parseLong(time_str.substring(0, len - 1)));
    else if (time_str.endsWith("m"))  return TimeValue.minutes(Long.parseLong(time_str.substring(0, len - 1)));
    else if (time_str.endsWith("s"))  return TimeValue.seconds(Long.parseLong(time_str.substring(0, len - 1)));
    else if (time_str.endsWith("ms")) return TimeValue.milliseconds(Long.parseLong(time_str.substring(0, len - 2)));
    else if (time_str.endsWith("us")) return TimeValue.microseconds(Long.parseLong(time_str.substring(0, len - 2)));
    else if (time_str.endsWith("ns")) return TimeValue.nanoseconds(Long.parseLong(time_str.substring(0, len - 2)));
    else                              return TimeValue.seconds(10);
  }

  @Override
  final protected int haveFun() throws Exception {
    ChainedOptionsBuilder options_builder =
        new OptionsBuilder().include(this.getClass().getSimpleName())
                            .forks(forks.value())
                            .warmupIterations(warmup_iterations.value()).warmupTime(warmup_t)
                            .measurementIterations(measure_iterations.value()).measurementTime(measure_t)
                            .threads(threads.value()).mode((Mode) mode.value());
    if (gc_profiler.value()) options_builder.addProfiler(GCProfiler.class);
    decorateOptions(options_builder);
    new Runner(options_builder.build()).run();
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  final protected void destroyToy() throws Exception {
  }

  abstract protected void decorateOptions(ChainedOptionsBuilder options_builder);

}
