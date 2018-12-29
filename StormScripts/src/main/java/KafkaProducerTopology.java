/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class KafkaProducerTopology {
    /**
     * @param brokerUrl Kafka broker URL
     * @param topicName Topic to which publish sentences
     * @return A Storm topology that produces random sentences using {@link RandomSentenceSpout} and uses a {@link KafkaBolt} to
     * publish the sentences to the kafka topic specified
     */
    public static StormTopology newTopology(String brokerUrl, String topicName) {
        final TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig<String,String> kafkaConf = KafkaSpoutConfig
                .builder("localhost:9092", Arrays.asList("metrics".split(",")))
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setTupleTrackingEnforced(true)
                .build();


        // Create the initial spoutConfig
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("localhost:2181"),
                "metrics",      // Kafka topic to read from
                "/metrics", // Root path in Zookeeper for the spout to store consumer offsets
                UUID.randomUUID().toString());  // ID for storing consumer offsets in Zookeeper

        // Set the scheme
        try {
            spoutConfig.scheme = new SchemeAsMultiScheme(getSchemeFromClassName("kafka.spout.scheme.class"));
        } catch(Exception e) {
            e.printStackTrace();
        }

        // Create the kafkaSpout
        KafkaSpout kafkaSpout =  new KafkaSpout(kafkaConf);

        builder.setSpout("spout", kafkaSpout, 2);

        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic. */
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(newProps(brokerUrl, topicName))
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .<String, String>withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String,String>("key", "word"));

        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
        }};
    }

    private static Scheme getSchemeFromClassName(String spoutSchemeCls) throws Exception {
        return (Scheme)Class.forName(spoutSchemeCls).getConstructor().newInstance();
    }
}


