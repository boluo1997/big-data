package com.boluo.dao;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author chao
 * @datetime 2025-02-03 20:13
 * @description
 */
@Component
public class KafkaTopicImpl {

    @Autowired
    private Environment environment;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicImpl.class);

    public Properties streamingKafkaProp2Standard(Map<String, String> kafkaParams) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaParams.get("kafka.group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaParams.get("value.deserializer"));

        properties.put("schema.registry.url", kafkaParams.get("schema.registry.url"));

        // cert files
        properties.put("security.protocol", kafkaParams.get("kafka.security.protocol"));
        properties.put("sasl.mechanism", kafkaParams.get("kafka.sasl.mechanism"));
        properties.put("sasl.jaas.config", kafkaParams.get("kafka.sasl.jaas.config"));

        properties.put("ssl.truststore.location", kafkaParams.get("kafka.ssl.truststore.location"));
        properties.put("ssl.truststore.password", kafkaParams.get("kafka.ssl.truststore.password"));

        return properties;
    }

    public void getKafkaTopicInfo(Map<String, String> kafkaParams) {

        Properties properties = streamingKafkaProp2Standard(kafkaParams);
        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        List<String> jobNames = Lists.newArrayList(
                "job_forte_pnl",
                "job_forte_trade",
                "job_forte_original_trade",
                "job_forte_echelle",
                "job_forte_rate",
                "job_forte_eod",
                "job_oms_direct",
                "job_oms_indirect"
        );

        for (String jobName : jobNames) {
            String topicName = environment.getProperty(jobName + ".topic");
            getPartitionsForTopic(jobName, topicName, consumer);
        }
        consumer.close();
    }


    public void getPartitionsForTopic(String jobName, String topicName, Consumer<Long, String> consumer) {
        // get partition info
        List<PartitionInfo> partitions = consumer.partitionsFor(topicName)
                .stream()
                .sorted(Comparator.comparingInt(PartitionInfo::partition))
                .collect(Collectors.toList());

        LOG.warn("\n");
        LOG.warn("Job: {}", jobName);
        LOG.warn("Topic: {} has {} partitions.", topicName, partitions.size());

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList()));

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList()));

        for (PartitionInfo partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
            long beginningOffset = beginningOffsets.get(topicPartition);
            long endOffset = endOffsets.get(topicPartition);
            LOG.warn("partition {}: beginning offset = {}, end offset = {}", partition.partition(), beginningOffset, endOffset);
        }

    }


    public List<Integer> getPartitions(String topicName, Map<String, String> kafkaParams) {
        Properties properties = streamingKafkaProp2Standard(kafkaParams);
        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        return consumer.partitionsFor(topicName).stream().sorted(Comparator.comparingInt(PartitionInfo::partition)).map(PartitionInfo::partition).collect(Collectors.toList());
    }

}
