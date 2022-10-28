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

import org.apache.aries.common.RETURN_CODE;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.ShortParameter;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.StringParameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicCreation extends AbstractKafkaToy {

  private final Parameter<String[]> topics =
      StringArrayParameter.newBuilder("tc.topics_creating").setRequired()
                          .setDescription("Topics to be created. All configurations are shared among these topics").opt();
  private final Parameter<Integer> partitions =
      IntParameter.newBuilder("tc.partitions").setRequired().setDescription("Partitions for each topics").opt();
  private final Parameter<Short> replications =
      ShortParameter.newBuilder("tc.replications").setRequired().setDescription("Replications for each partitions").opt();
  private final Parameter<String> retention_ms =
      StringParameter.newBuilder("tc.retention_ms").setDescription("Retention time in ms for topics").opt();
  private final Parameter<String> retention_bytes =
      StringParameter.newBuilder("tc.retention_bytes").setDescription("Retentions bytes for topics").opt();

  private final Map<String, String> configs = new HashMap<>();

  private AdminClient ac;
  private int topic_partitions;
  private short partition_replications;

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(topics);
    requisites.add(partitions);
    requisites.add(replications);
    requisites.add(retention_ms);
    requisites.add(retention_bytes);
  }

  @Override
  protected void exampleConfiguration() {
    example(topics.key(), "stream.topic,for.metrics");
    example(partitions.key(), "3");
    example(replications.key(), "2");
    example(retention_ms.key(), "66666666");
    example(retention_bytes.key(), "2000000");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    if (retention_ms.empty() || retention_bytes.empty()) {
      throw new IllegalArgumentException(retention_ms.key() + " or " + retention_bytes.key() +
          " at least one of these two must be configed.");
    }
    configs.put(TopicConfig.RETENTION_MS_CONFIG, retention_ms.value());
    if (!retention_bytes.empty()) {
      configs.put(TopicConfig.RETENTION_BYTES_CONFIG, retention_bytes.value());
    }

    ac = AdminClient.create(configuration.getProperties());
    topic_partitions = partitions.value();
    partition_replications = replications.value();
  }

  @Override
  protected int haveFun() throws Exception {

    List<NewTopic> new_topics = new ArrayList<>(topics.value().length);
    for (String topic : topics.value()) {
      new_topics.add(new NewTopic(topic, topic_partitions, partition_replications).configs(configs));
    }
    CreateTopicsResult ctr = ac.createTopics(new_topics);
    ctr.all().get();
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  protected void destroyToy() throws Exception {
    ac.close(5, TimeUnit.SECONDS);
  }

  @Override protected String getParameterPrefix() {
    return "tc";
  }

}
