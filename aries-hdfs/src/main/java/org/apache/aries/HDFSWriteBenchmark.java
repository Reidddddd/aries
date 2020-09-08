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

import org.apache.aries.common.Constants;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.List;
import java.util.Random;

public class HDFSWriteBenchmark extends HDFSBenchmark {

  private final Parameter<Integer> write_size =
      IntParameter.newBuilder("bm.hdfs.write.file_size_in_mb").setDefaultValue(128).setDescription("What is the size of a file to be writtien on HDFS").opt();
  private final Parameter<Integer> write_buffer_size =
      IntParameter.newBuilder("bm.hdfs.write.buffer_size_in_bytes").setDefaultValue(Constants.ONE_KB).setDescription("What is the size of each write's buffer").opt();

  private long size_in_bytes;
  private byte[] bytes;

  @Override
  protected void requisite(List<Parameter> requisites) {
    super.requisite(requisites);
    requisites.add(write_size);
    requisites.add(write_buffer_size);
  }

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    size_in_bytes = write_size.value() * Constants.ONE_MB;
    bytes = generateBytes();
  }

  @Override
  protected void exampleConfiguration() {
    super.exampleConfiguration();
    example(write_size.key(), "128");
    example(write_buffer_size.key(), "1024");
  }

  @Override protected void destroyToy() throws Exception {
    super.destroyToy();
  }

  @Benchmark
  public void testHDFSWrite() throws Exception {
    FSDataOutputStream os = null;
    try {
      Path out = new Path(work_dir, generateRandomString());
      os = file_system.create(out);
      long written_bytes = 0;
      while (written_bytes < size_in_bytes) {
        os.write(switchOneByte(bytes));
        written_bytes += bytes.length;
      }
    } finally {
      if (os != null) {
        os.close();
      }
    }
  }

  private String generateRandomString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      builder.append(nums[random.nextInt(10)]);
    }
    return builder.toString();
  }

  private byte[] switchOneByte(byte[] bytes) {
    int len = bytes.length;
    bytes[random.nextInt(len)] = Byte.decode(nums[random.nextInt(10)]);
    return bytes;
  }

  private byte[] generateBytes() {
    byte[] array = new byte[write_buffer_size.value()];
    for (int i = 0; i < write_buffer_size.value(); i++) {
      array[i] = Byte.decode(nums[random.nextInt(10)]);
    }
    return array;
  }

  final Random random = new Random();
  final String[] nums = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};

}
