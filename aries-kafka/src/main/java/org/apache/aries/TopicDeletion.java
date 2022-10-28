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
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicDeletion extends AbstractKafkaToy {

  private final Parameter<String[]> topics =
      StringArrayParameter.newBuilder("td.delete_topics")
                          .setRequired().setDescription("Topics to be deleted.")
                          .addConstraint(v -> v.length >= 1)
                          .opt();
  private final Parameter<Integer> partitions_upper =
      IntParameter.newBuilder("td.partitions_upper_limit").setRequired()
                  .setDescription("Each deletions will not delete more than this number of partitions")
                  .opt();

  private AdminClient ac;

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(topics);
    requisites.add(partitions_upper);
  }

  @Override
  protected void exampleConfiguration() {
    example(topics.key(), "a,b,c,d");
    example(partitions_upper.key(), "200");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    ac = AdminClient.create(configuration.getProperties());
  }

  @Override
  protected int haveFun() throws Exception {
    List<String> on_list = Arrays.asList(topics.value());
    List<String> to_be_deleted = new ArrayList<>();
    int upper_limit = partitions_upper.value();

    DescribeTopicsResult describe_result = ac.describeTopics(on_list);
    Map<String, KafkaFuture<TopicDescription>> results = describe_result.values();
    for (String topic : on_list) {
      TopicDescription topic_description = null;
      try {
        topic_description = results.get(topic).get();
      } catch (Exception utope) {
        LOG.warning(topic + " doesn't exist");
        continue;
      }
      if (topic_description.isInternal()) {
        LOG.warning("Skipping internal topic " + topic);
        continue;
      }
      if (upper_limit > 0) {
        int num_partitions = topic_description.partitions().size();
        LOG.info("Topic " + topic + " has " + num_partitions + " partitions");
        LOG.info("Deleting topic " + topic);
        to_be_deleted.add(topic);
        upper_limit -= num_partitions;
      }
      if (upper_limit <= 0) break;
    }

    DeleteTopicsResult delete_result = ac.deleteTopics(to_be_deleted);
    delete_result.all().get();
    return RETURN_CODE.SUCCESS.code();
  }

  @Override
  protected void destroyToy() throws Exception {
    ac.close(5, TimeUnit.SECONDS);
  }

  @Override protected String getParameterPrefix() {
    return "td";
  }

}
